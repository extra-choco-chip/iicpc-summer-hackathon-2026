import asyncio
import aiohttp
import time

TARGET_URL = "http://localhost:8080"
TOTAL_REQUESTS = 100

async def fire_order(bot_id, session):
    # Dummy trading payload
    payload = {"symbol": "BTCUSD", "side": "BUY", "qty": 1, "price": 50000}
    
    start_time = time.time() # ⏱️ Stopwatch START
    
    try:
        async with session.post(TARGET_URL, json=payload) as response:
            status = response.status
            await response.text() # Read the receipt to fully close the connection
            
            end_time = time.time() # ⏱️ Stopwatch END
            latency = end_time - start_time
            
            print(f"Bot {bot_id:03d} | Status: {status} | Latency: {latency:.4f}s")
            return latency
            
    except Exception as e:
        print(f"Bot {bot_id:03d} | Failed connection: {e}")
        return None

async def main():
    print(f"🚀 Launching fleet of {TOTAL_REQUESTS} bots at {TARGET_URL}...\n")
    
    # Open ONE continuous connection manager (like our WebSocket phone call analogy, but for REST)
    async with aiohttp.ClientSession() as session:
        tasks = []
        
        # Line up all 100 bots at the starting line
        for i in range(TOTAL_REQUESTS):
            tasks.append(fire_order(i, session))
            
        # FIRE THE GUN! Run them all concurrently
        results = await asyncio.gather(*tasks)
        
        # Calculate a quick average for the terminal
        latencies = [r for r in results if r is not None]
        if latencies:
            avg_latency = sum(latencies) / len(latencies)
            print(f"\n✅ Fleet Finished! Average Latency: {avg_latency:.4f}s")

if __name__ == "__main__":
    asyncio.run(main())