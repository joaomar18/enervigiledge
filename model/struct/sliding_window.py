###########EXTERNAL IMPORTS############

from collections import deque
from typing import TypeVar, Generic, Deque, List

#######################################

#############LOCAL IMPORTS#############

#######################################

T = TypeVar("T")


class SlidingWindow(Generic[T]):
    """
    Fixed-size sliding window for storing recent values.

    Maintains the most recent items up to a maximum size, automatically
    discarding the oldest entries when new items are added.
    """

    def __init__(self, max_size: int) -> None:
        self.max_size = max_size
        self.items: Deque[T] = deque(maxlen=max_size)

    def add(self, item: T) -> None:
        """
        Adds a new item to the window.

        The item becomes the most recent entry. If the window is full,
        the oldest item is discarded.
        """

        self.items.appendleft(item)

    def peek(self, index: int = 0) -> T:
        """
        Returns an item without removing it.

        Args:
            index: Position in the window (0 = most recent).

        Returns:
            The requested item.
        """

        return self.items[index]

    def pop_left(self) -> T:
        """
        Removes and returns the most recent item.

        Returns:
            The most recently added item.
        """

        return self.items.popleft()

    def pop_right(self) -> T:
        """
        Removes and returns the oldest item.

        Returns:
            The oldest item in the window.
        """

        return self.items.pop()

    def get_list(self) -> List[T]:
        """
        Returns the current window contents as a list.

        Returns:
            A list of items ordered from most recent to oldest.
        """

        return list(self.items)
