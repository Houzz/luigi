import collections
from datetime import datetime
import time

import luigi
from luigi import six
from luigi.notifications import send_email, send_error_email, email
import luigi.parameter


class batch_email(luigi.Config):
    email_interval = luigi.parameter.IntParameter(
        default=60, description='Number of minutes between e-mail sends (default: 60)')
    batch_mode = luigi.parameter.ChoiceParameter(
        default='unbatched_params', choices=('family', 'all', 'unbatched_params'),
        description='Method used for batching failures in e-mail. If "family" all failures for '
                    'tasks with the same family will be batched. If "unbatched_params", all '
                    'failures for tasks with the same family and non-batched parameters will be '
                    'batched. If "all", tasks will only be batched if they have identical names. '
                    '(default: unbatched_params)')
    error_lines = luigi.parameter.IntParameter(
        default=0, description='Number of lines to show from each error message. Default: all')
    error_messages = luigi.parameter.IntParameter(
        default=0, description='Number of error messages to show for each group')
    group_by_error_messages = luigi.parameter.BoolParameter(
        default=False, description='Group items with the same error messages together')


class ExplQueue(collections.OrderedDict):
    def __init__(self, num_items):
        self.num_items = num_items
        super(ExplQueue, self).__init__()

    def enqueue(self, item):
        self.pop(item, None)
        self[item] = datetime.now()
        if len(self) > self.num_items:
            self.popitem(last=False)  # pop first item if past length


def _fail_queue(num_messages):
    return lambda: collections.defaultdict(lambda: ExplQueue(num_messages))


def plural_format(template, number, plural='s', empty_on_zero=True):
    if number == 0 and empty_on_zero:
        return ''
    return template.format(number, '' if number == 1 else plural)


class BatchNotifier(object):
    def __init__(self, **kwargs):
        self._config = batch_email(**kwargs)
        self._fail_counts = collections.defaultdict(collections.Counter)
        self._disabled_counts = collections.defaultdict(collections.Counter)
        self._scheduling_fail_counts = collections.defaultdict(collections.Counter)
        self._fail_expls = collections.defaultdict(_fail_queue(self._config.error_messages))
        self._update_next_send()

        self._email_format = email().format
        if email().receiver:
            self._default_owner = set(filter(None, email().receiver.split(',')))
        else:
            self._default_owner = set()

    def _update_next_send(self):
        self._next_send = time.time() + 60 * self._config.email_interval

    def _key(self, task_name, family, unbatched_args):
        if self._config.batch_mode == 'all':
            return task_name
        elif self._config.batch_mode == 'family':
            return family
        elif self._config.batch_mode == 'unbatched_params':
            param_str = ', '.join('{}={}'.format(*kv) for kv in six.iteritems(unbatched_args))
            return '{}({})'.format(family, param_str)
        else:
            raise ValueError('Unknown batch mode for batch notifier: {}'.format(
                self._config.batch_mode))

    def _format_expl(self, expl):
        lines = expl.rstrip().split('\n')[-self._config.error_lines:]
        if self._email_format == 'html':
            return '<pre>{}</pre>'.format('\n'.join(lines))
        else:
            return '\n{}'.format('\n'.join(map('      {}'.format, lines)))

    def _expl_body(self, expls):
        lines = [self._format_expl(expl) for expl in expls]
        if lines and self._email_format != 'html':
            lines.append('')
        return '\n'.join(lines)

    def _format_task(self, (task, failure_count, disable_count, scheduling_count)):
        disabled_line = plural_format(', {} disable{}', disable_count)
        fail_line = plural_format('{} failure{}', failure_count, empty_on_zero=False)
        scheduling_line = plural_format(', {} scheduling failure{}', scheduling_count)
        return '{} ({}{}{})'.format(task, fail_line, disabled_line, scheduling_line)

    def _format_tasks(self, tasks):
        lines = map(self._format_task, sorted(tasks, key=self._expl_key))
        if self._email_format == 'html':
            return '<li>{}'.format('\n<br>'.join(lines))
        else:
            return '- {}'.format('\n  '.join(lines))

    def _owners(self, owners):
        return self._default_owner | set(owners or ())

    def add_failure(self, task_name, family, unbatched_args, expl, owners=None):
        key = self._key(task_name, family, unbatched_args)
        for owner in self._owners(owners):
            self._fail_counts[owner][key] += 1
            self._fail_expls[owner][key].enqueue(expl)

    def add_disable(self, task_name, family, unbatched_args, owners=None):
        key = self._key(task_name, family, unbatched_args)
        for owner in self._owners(owners):
            self._disabled_counts[owner][key] += 1
            self._fail_counts[owner].setdefault(key, 0)

    def add_scheduling_fail(self, task_name, family, unbatched_args, expl, owners=None):
        key = self._key(task_name, family, unbatched_args)
        for owner in self._owners(owners):
            self._scheduling_fail_counts[owner][key] += 1
            self._fail_expls[owner][key].enqueue(expl)
            self._fail_counts[owner].setdefault(key, 0)

    def _task_expl_groups(self, expls):
        if not self._config.group_by_error_messages:
            return [((expl,), msg) for expl, msg in six.iteritems(expls)]

        groups = collections.defaultdict(list)
        for expl, msg in six.iteritems(expls):
            groups[msg].append(expl)
        return [(expls, msg) for msg, expls in six.iteritems(groups)]

    def _expls_key(self, (expls, _)):
        num_failures = sum(failures + scheduling_fails for (_1, failures, _2, scheduling_fails) in expls)
        num_disables = sum(disables for (_1, _2, disables, _3) in expls)
        max_name = max(expls)[0]
        return -num_failures, -num_disables, max_name

    def _expl_key(self, expl):
        return self._expls_key(((expl,), None))

    def _email_body(self, fail_counts, disable_counts, scheduling_counts, fail_expls):
        expls = {
            (name, fail_count, disable_counts[name], scheduling_counts[name]): self._expl_body(fail_expls[name])
            for name, fail_count in six.iteritems(fail_counts)
        }
        expl_groups = sorted(self._task_expl_groups(expls), key=self._expls_key)
        body_lines = []
        for tasks, msg in expl_groups:
            body_lines.append(self._format_tasks(tasks))
            body_lines.append(msg)
        body = '\n'.join(filter(None, body_lines)).rstrip()
        if self._email_format == 'html':
            return '<ul>\n{}\n</ul>'.format(body)
        else:
            return body

    def _send_email(self, fail_counts, disable_counts, scheduling_counts, fail_expls, owner):
        num_failures = sum(six.itervalues(fail_counts))
        if num_failures > 0 or disable_counts or scheduling_counts:
            plural_s = 's' if num_failures != 1 else ''
            prefix = '' if owner in self._default_owner else 'Your tasks have '
            subject = 'Luigi: {}{} failure{} in the last {} minutes'.format(
                prefix, num_failures, plural_s, self._config.email_interval)
            email_body = self._email_body(fail_counts, disable_counts, scheduling_counts, fail_expls)
            send_email(subject, email_body, email().sender, (owner,))

    def send_email(self):
        for owner, failures in six.iteritems(self._fail_counts):
            self._send_email(
                fail_counts=failures,
                disable_counts=self._disabled_counts[owner],
                scheduling_counts=self._scheduling_fail_counts[owner],
                fail_expls=self._fail_expls[owner],
                owner=owner,
        )
        self._update_next_send()
        self._fail_counts.clear()
        self._disabled_counts.clear()
        self._scheduling_fail_counts.clear()
        self._fail_expls.clear()

    def update(self):
        if time.time() >= self._next_send:
            self.send_email()
