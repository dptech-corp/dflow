import warnings
from typing import Union, List, Any

dflow = Any


class _Context(object):
    """Global Context Manager."""

    def __init__(self):
        """Init context with default to false."""
        self._in_context = False
        self.current_workflow = None

    @property
    def in_context(self) -> bool:
        """whether it is in context environment or not."""
        return self._in_context

    @in_context.setter
    def in_context(self, value: bool):
        if not isinstance(value, bool):
            raise ValueError(f'Unsupported in_context value: {value}.'
                             f'Expected bool, got {type(value)}')
        if not value:
            self._reset()
        self._in_context = value

    def _reset(self):
        """Reset context to origin condition.

        (for context exit usage only.)
        """
        self._in_context = False
        self.current_workflow = None

    def to_in_context(self):
        """Switch to be in context environment."""
        if self.in_context:
            warnings.warning(
                'Already in context. But call `to_in_context()` again.')
        self._in_context = True

    def to_out_context(self):
        """Switch to be not in context environment."""
        if not self.in_context:
            warnings.warning(
                'Not in context. But call `to_out_context()` again.')
        self._in_context = False

    def registry_step(self,
                      step: Union['dflow.Step',
                                  List['dflow.Step'],
                                  'dflow.Task',
                                  List['dflow.Task']]):
        """Registry step to context."""
        self.current_workflow.add(step)


GLOBAL_CONTEXT = _Context()


class Range_Context(object):
    """Local context for range."""

    def __init__(self):
        """Init context with default to false."""
        self.range_param_name = None
        self.range_target_name = None
        self._in_context = False
        self.current_step = None
        self.range_param_len = 0

    @property
    def in_context(self) -> bool:
        """whether it is in context environment or not."""
        return self._in_context

    @in_context.setter
    def in_context(self, value: bool):
        if not isinstance(value, bool):
            raise ValueError(f'Unsupported in_context value: {value}.'
                             f'Expected bool, got {type(value)}')
        if not value:
            self._reset()
        self._in_context = value

    def _reset(self):
        """Reset context to origin condition.

        (for context exit usage only.)
        """
        self._in_context = False
        self.current_workflow = None
        self.range_param_name = None
        self.range_target_name = None
        self.range_param_len = 0

    def to_in_context(self):
        """Switch to be in context environment."""
        if self.in_context:
            warnings.warning(
                'Already in context. But call `to_in_context()` again.')
        self._in_context = True

    def to_out_context(self):
        """Switch to be not in context environment."""
        if not self.in_context:
            warnings.warning(
                'Not in context. But call `to_out_context()` again.')
        self._in_context = False

    def get_current_range_param_name(self):
        if self.range_param_name:
            return self.range_param_name
        else:
            raise SyntaxError("It seems not right.")


GLOBAL_RANGE_CONTEXT = Range_Context()
