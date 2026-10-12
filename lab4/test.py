import asyncio
from throttler import Throttle, TooManyRequest


async def worker(item_id: int):
    await asyncio.sleep(0.02)
    return f"Дані #{item_id}"


async def demo_token_bucket():
    print("\n--- [Token Bucket]: capacity=3, refillRate=1 token/sec ---")
    bucket = Throttle(leading=True, trailing=False, capacity=3, refillRate=1.0)
    throttled_fn = bucket.throttle(worker)

    tasks = [throttled_fn(i) for i in range(4)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for i, res in enumerate(results):
        status = "ВІДХИЛЕНО" if isinstance(res, TooManyRequest) else f"OK ({res})"
        print(f"  Сплеск #{i}: {status}")

    print("  ...очікування 1.1с...")
    await asyncio.sleep(1.1)

    res = await asyncio.gather(throttled_fn(99), return_exceptions=True)
    print(f"  Після очікування #99: {'OK' if not isinstance(res[0], Exception) else 'ВІДХИЛЕНО'}")


async def demo_sliding_window():
    print("\n--- [Sliding Window]: limit=2 виклики на 1.0 секунду ---")
    window = Throttle(leading=True, trailing=False, rate=(2, 1.0))
    throttled_fn = window.throttle(worker)

    tasks = [throttled_fn(i) for i in range(3)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for i, res in enumerate(results):
        status = "ВІДХИЛЕНО" if isinstance(res, TooManyRequest) else f"OK ({res})"
        print(f"  Запит #{i}: {status}")

    print("  ...очікування 1.05с (вікно очиститься)...")
    await asyncio.sleep(1.05)

    res = await throttled_fn(100)
    print(f"  Новий запит #100: OK ({res})")


async def demo_leading_false_trailing_true():
    print("\n--- [leading=False, trailing=True]: Відкладений виклик лише в кінці ---")
    t = Throttle(leading=False, trailing=True, rate=(1, 0.5))
    throttled_fn = t.throttle(worker)

    r1 = await throttled_fn(1)
    r2 = await throttled_fn(2)
    r3 = await throttled_fn(3)
    print(f"  Результати прямого виклику (всі None): {r1}, {r2}, {r3}")

    print("  ...чекаємо виконання фонового trailing (0.6с)...")
    await asyncio.sleep(0.6)


async def demo_leading_true_trailing_true():
    print("\n--- [leading=True, trailing=True]: Класичний UI-тротлер ---")
    
    t = Throttle(leading=True, trailing=True, rate=(1, 0.5))
    throttled_fn = t.throttle(worker)

    r1 = await throttled_fn(1)  
    r2 = await throttled_fn(2)  
    r3 = await throttled_fn(3) 
    print(f"  Прямі виклики: r1={r1}, r2={r2}, r3={r3}")

    print("  ...чекаємо завершення інтервалу (0.6с)...")
    await asyncio.sleep(0.6)


async def demo_leading_false_trailing_false():
    print("\n--- [leading=False, trailing=False]: Дроп/Блокування всього ---")
    t = Throttle(leading=False, trailing=False, rate=(1, 0.5))
    throttled_fn = t.throttle(worker)

    try:
        await throttled_fn(1)
    except TooManyRequest:
        print("  Запит 1: Одразу відхилено (немає куди передавати виконання)")



async def main():

    await demo_token_bucket()
    await demo_sliding_window()


    await demo_leading_false_trailing_true()
    await demo_leading_true_trailing_true()
    await demo_leading_false_trailing_false()

asyncio.run(main())