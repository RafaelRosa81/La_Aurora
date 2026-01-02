from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

TZ_UY = ZoneInfo("America/Montevideo")


def to_epoch_ms(dt: datetime) -> int:
    """
    Convierte datetime (aware) a epoch en milisegundos.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ_UY)
    return int(dt.timestamp() * 1000)


def month_range(start_date: datetime, end_date: datetime):
    """
    Genera tuplas (start_of_month, end_of_month) entre dos fechas.
    Ambos inclusive.
    """
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=TZ_UY)
    if end_date.tzinfo is None:
        end_date = end_date.replace(tzinfo=TZ_UY)

    current = start_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    while current <= end_date:
        # inicio del mes
        month_start = current

        # inicio del mes siguiente
        if current.month == 12:
            next_month = current.replace(year=current.year + 1, month=1)
        else:
            next_month = current.replace(month=current.month + 1)

        # fin del mes (último milisegundo)
        month_end = next_month - timedelta(milliseconds=1)

        yield month_start, min(month_end, end_date)

        current = next_month


def parse_date(date_str: str) -> datetime:
    """
    Parsea fecha YYYY-MM-DD y la devuelve timezone-aware (Montevideo).
    """
    return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=TZ_UY)
