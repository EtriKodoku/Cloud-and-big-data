import asyncio
import time
from functools import wraps

def timeout(seconds: float):
    def decorator(fn):
        @wraps(fn)
        async def wrapper(*args, **kwargs):
            async with asyncio.timeout(seconds):
                return await fn(*args, **kwargs)

        return wrapper

    return decorator


def retry(max_attempts: int, delay: float = 0.1):
    def decorator(fn):
        @wraps(fn)
        async def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return await fn(*args, **kwargs)
                except Exception as e:
                    print(
                        f"      [Retry] Спроба {attempt}/{max_attempts} зазнала збою: {e}"
                    )
                    if attempt == max_attempts:
                        raise
                    print(
                        f"      [Retry] Пауза {delay}c перед наступною спробою..."
                    )
                    await asyncio.sleep(delay)

        return wrapper

    return decorator



async def demo_1_success_before_timeout():
    print("=" * 60)
    print("ТЕСТ 1: Успішна операція до тайм-ауту (Ліміт: 1.0с, Робота: 0.3с)")
    print("=" * 60)

    @timeout(1.0)
    async def fetch_user_data():
        print("  -> Запуск завантаження даних...")
        await asyncio.sleep(0.3)
        print("  -> Дані успішно завантажено!")
        return {"user_id": 42, "status": "active"}

    start = time.monotonic()
    try:
        data = await fetch_user_data()
        elapsed = time.monotonic() - start
        print(f"  [РЕЗУЛЬТАТ] Успіх! Отримано: {data}")
        print(f"  [ЧАС] Витрачено: {elapsed:.3f}с (менше за ліміт 1.0с)\n")
    except TimeoutError:
        print("  [ПОМИЛКА] Несподіваний тайм-аут!\n")


async def demo_2_timeout_and_side_effects():
    print("=" * 60)
    print(
        "ТЕСТ 2: Перевищення ліміту та перевірка скасування побічних ефектів"
    )
    print("        (Ліміт: 0.5с, Операція триває: 1.5с)")
    print("=" * 60)

    database_record_created = False

    @timeout(0.5)
    async def transfer_money():
        nonlocal database_record_created
        print("  -> Крок 1: Початок банківської транзакції...")
        print("  -> Крок 2: Очікування відповіді платіжного шлюзу (1.5с)...")

        await asyncio.sleep(1.5)

        database_record_created = True
        print("  -> Крок 3: [ПОБІЧНИЙ ЕФЕКТ] Запис транзакції внесено в базу!")
        return "Транзакція успішна"

    start = time.monotonic()
    try:
        await transfer_money()
    except TimeoutError:
        elapsed = time.monotonic() - start
        print(
            f"  [ТАЙМ-АУТ] Перехоплено TimeoutError рівно через {elapsed:.3f}с!"
        )

    # Явна перевірка стану системи після скасування
    print("  [ПЕРЕВІРКА СТАНУ]:")
    if not database_record_created:
        print(
            "   Справдилося: побічний ефект скасовано, кошти НЕ списано, базу НЕ змінено.\n"
        )
    else:
        print("   Помилка: транзакція завершилася всупереч тайм-ауту!\n")


async def demo_3_timeout_combined_with_retry():
    print("=" * 60)
    print("ТЕСТ 3: Комбінування з Retry (контроль загального часу)")
    print("        (Загальний тайм-аут: 0.6с, Спроб: 5 по ~0.3с кожна)")
    print("        (Без тайм-ауту 5 спроб тривали б 5 * 0.3с = 1.5с)")
    print("=" * 60)

    attempts_count = 0


    @timeout(0.6)
    @retry(max_attempts=5, delay=0.1)
    async def call_unstable_service():
        nonlocal attempts_count
        attempts_count += 1
        print(f"  -> Виклик зовнішнього сервісу (Спроба №{attempts_count})...")
        await asyncio.sleep(0.2) 
        raise ConnectionResetError("Сервіс недоступний (503)")

    start = time.monotonic()
    try:
        await call_unstable_service()
    except TimeoutError:
        elapsed = time.monotonic() - start
        print(
            f"\n  [ТАЙМ-АУТ] Загальний бюджет часу вичерпано через {elapsed:.3f}с!"
        )
        print(f"  [ДЕТАЛІ]: Виконано лише {attempts_count} з 5 спроб.")
        print(
            "   Справдилося: ретраї не розмножили час до 1.5с, а зупинилися за лімітом 0.6с.\n"
        )
    except ConnectionResetError:
        print(
            "  [ПОМИЛКА] Вилетіла помилка з'єднання замість загального тайм-ауту.\n"
        )


async def demo_4_retry_combined_with_timeout():
    print("=" * 60)
    print("ТЕСТ 4: Індивідуальний тайм-аут на кожну спробу (@retry над @timeout)")
    print("        (Ліміт спроби: 0.2с, Спроби 1-2 зависають на 0.5с, Спроба 3 успішна)")
    print("=" * 60)

    attempts_count = 0

    @retry(max_attempts=3, delay=0.1)
    @timeout(0.2)
    async def fetch_with_individual_timeout():
        nonlocal attempts_count
        attempts_count += 1
        print(f"  -> Запуск спроби №{attempts_count}...")

        if attempts_count < 3:
            await asyncio.sleep(0.5)
        else:
            await asyncio.sleep(0.05)  
            return {"status": "success", "recovered_on_attempt": attempts_count}

    start = time.monotonic()
    try:
        data = await fetch_with_individual_timeout()
        elapsed = time.monotonic() - start
        print(f"\n  [РЕЗУЛЬТАТ] Успіх! Отримано: {data}")
        print(
            f"  [ЧАС] Загальний час: {elapsed:.3f}с "
            f"(2 невдачі по ~0.2с + 2 паузи по 0.1с + 1 швидка спроба)\n"
        )
    except TimeoutError:
        print("  [ПОМИЛКА] Всі спроби вичерпано через тайм-аут!\n")


async def main():
    await demo_1_success_before_timeout()
    await demo_2_timeout_and_side_effects()
    await demo_3_timeout_combined_with_retry()
    await demo_4_retry_combined_with_timeout()


if __name__ == "__main__":
    asyncio.run(main())