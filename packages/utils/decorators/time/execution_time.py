import time
from functools import wraps
from typing import Callable, Any

__ALL__: list[str] = ['execution_time']


def format_duration(seconds: float) -> str:
    """
    Форматирует время в секундах в удобочитаемый строковый формат.

    Формат вывода зависит от продолжительности:
    - Если время больше часа: "h час m мин s сек".
    - Если время больше минуты: "m мин s сек".
    - Иначе: "s сек".

    Args:
        seconds (float): Время в секундах.

    Returns:
        str: Отформатированная строка времени.
    """
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    
    if hours > 0:
        return f"{hours} час {minutes} мин {secs} сек"
    elif minutes > 0:
        return f"{minutes} мин {secs} сек"
    else:
        return f"{secs} сек"

def execution_time(func: Callable[..., Any]) -> Callable[..., Any]:
    """
    Декоратор для замера времени выполнения функции.

    После выполнения функции выводит время её выполнения в формате,
    зависящем от продолжительности (см. `format_duration`).

    Args:
        func (Callable[..., Any]): Функция, время выполнения которой нужно замерить.

    Returns:
        Callable[..., Any]: Обёрнутая функция с добавленным замером времени.
    """
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        duration = end_time - start_time
        formatted_duration = format_duration(duration)
        print(f"Функция '{func.__name__}' выполнилась за {formatted_duration}")
        return result
    return wrapper
