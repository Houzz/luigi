import collections
import time

import luigi
from luigi import six
from luigi.notifications import send_error_email
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


class BatchNotifier(object):
    def __init__(self, **kwargs):
        self._config = batch_email(**kwargs)
        self._fail_counts = collections.Counter()
        self._disabled_counts = collections.Counter()
        self._update_next_send()

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

    def add_failure(self, task_name, family, unbatched_args, expl):
        self._fail_counts[self._key(task_name, family, unbatched_args)] += 1

    def add_disable(self, task_name, family, unbatched_args):
        key = self._key(task_name, family, unbatched_args)
        self._disabled_counts[key] += 1
        self._fail_counts.setdefault(key, 0)

    def _email_body(self):
        body_lines = []
        for name, failure_count in self._fail_counts.most_common():
            if self._disabled_counts.get(name):
                disables = self._disabled_counts[name]
                disabled_line = ', 1 disable' if disables == 1 else ', {} disables'.format(disables)
            else:
                disabled_line = ''

            plural_s = '' if failure_count == 1 else 's'
            body_line = '{} ({} failure{}{})'.format(name, failure_count, plural_s, disabled_line)
            body_lines.append(body_line)
        return '\n'.join(body_lines)

    def send_email(self):
        num_failures = sum(six.itervalues(self._fail_counts))
        if num_failures > 0 or self._disabled_counts:
            plural_s = 's' if num_failures != 1 else ''
            subject = '{} failure{} in the last {} minutes'.format(
                num_failures, plural_s, self._config.email_interval)
            send_error_email(subject, self._email_body())
        self._update_next_send()
        self._fail_counts.clear()
        self._disabled_counts.clear()

    def update(self):
        if time.time() >= self._next_send:
            self.send_email()
