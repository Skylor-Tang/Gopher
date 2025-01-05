import asyncio
import aiohttp
import pandas as pd
import time

# 文件名
input_file = 'data.csv'
good_file = 'good.csv'
bad_file = 'bad.csv'

http_timeout = 10
max_concurrency = 500  # 最大并发数
request_delay = 0.1


async def check_url(session: aiohttp.ClientSession, url: str, semaphore: asyncio.Semaphore) -> bool:
    async with semaphore:
        try:
            async with session.get(url) as response:
                await asyncio.sleep(request_delay)
                return 200 <= response.status < 300
        except:
            return False


async def check_urls(df: pd.DataFrame, session: aiohttp.ClientSession, semaphore: asyncio.Semaphore):
    good_rows = []
    bad_rows = []

    async def process_row(row):
        url = row['发布链接']
        is_valid = await check_url(session, url, semaphore)
        if is_valid:
            good_rows.append(row)
        else:
            bad_rows.append(row)

    tasks = [process_row(row) for _, row in df.iterrows()]
    await asyncio.gather(*tasks)

    return good_rows, bad_rows

async def process_csv():
    df = pd.read_csv(input_file)

    # 获取“发布链接”列的索引位置
    if '发布链接' not in df.columns:
        print("没有找到发布链接列")
        return

    semaphore = asyncio.Semaphore(max_concurrency)  # 控制并发数
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=http_timeout)) as session:
        good_rows, bad_rows = await check_urls(df, session, semaphore)

        # 将结果写入 CSV 文件
        good_df = pd.DataFrame(good_rows)
        bad_df = pd.DataFrame(bad_rows)

        # 写入文件
        good_df.to_csv(good_file, index=False)
        bad_df.to_csv(bad_file, index=False)

        print(f"处理完毕，生成文件：{good_file} 和 {bad_file}")


def main():
    start_time = time.time()
    asyncio.run(process_csv())
    print(f"执行时间: {time.time() - start_time:.2f} 秒")

if __name__ == '__main__':
    main()
