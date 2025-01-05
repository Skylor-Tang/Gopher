package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	inputFile      = "data.csv"       // 输入 CSV 文件
	goodFile       = "good.csv"       // 正常的 CSV 文件
	badFile        = "bad.csv"        // 异常的 CSV 文件
	httpTimeout    = 10 * time.Second // 每个 HTTP 请求的超时时间
	maxConcurrency = 4000             // 最大并发数
	readBufferSize = 1024 * 64        // 64KB 缓冲区大小
	globalTimeout  = 10 * time.Minute // 全局超时设置
)

type SafeCounter struct {
	processed atomic.Value
}

func (sc *SafeCounter) Increment() {
	for {
		oldProcessed := sc.processed.Load()
		newProcessed := oldProcessed.(int) + 1

		if sc.processed.CompareAndSwap(oldProcessed, newProcessed) {
			break
		}
		// 失败重试
	}
}

func (sc *SafeCounter) Add(n int) {
	for {
		oldProcessed := sc.processed.Load()
		newProcessed := oldProcessed.(int) + n

		if sc.processed.CompareAndSwap(oldProcessed, newProcessed) {
			break
		}
		// 失败重试
	}
}

func (sc *SafeCounter) GetProcessed() int {
	return sc.processed.Load().(int)
}

var (
	// 安全的进度变量
	lineCount = &SafeCounter{}
)

// 创建 HTTP 客户端，启用持久连接
func createHttpClient() *http.Client {
	return &http.Client{
		Timeout: httpTimeout,
		Transport: &http.Transport{
			DisableKeepAlives: false, // 启用持久连接
		},
	}
}

// 检查 URL 是否有效
func checkURL(ctx context.Context, client *http.Client, url string) bool {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)

	if err != nil {
		return false
	}
	resp, err := client.Do(req)

	if err != nil {
		return false
	}

	defer func(Body io.ReadCloser) {
		if err := Body.Close(); err != nil {
			return
		}
	}(resp.Body)
	lineCount.Increment()

	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

// 批量写入文件
func writeCSVFile(writer *csv.Writer, rows [][]string) {
	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			return
		}
	}
	writer.Flush()
}

func countLines(filePath string) (int, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineCount := 0
	for scanner.Scan() {
		lineCount++
	}

	if err := scanner.Err(); err != nil {
		return 0, err
	}

	return lineCount, nil
}

// 处理 CSV 文件，过滤有效和无效链接
func processCSV(ctx context.Context) {
	lineCount.processed.Store(0)

	// 打开输入 CSV 文件
	file, err := os.Open(inputFile)
	if err != nil {
		fmt.Println("无法打开输入文件:", err)
		return
	}
	defer file.Close()

	// 创建输出 CSV 文件
	goodFile, err := os.Create(goodFile)
	if err != nil {
		fmt.Println("无法创建 good.csv 文件:", err)
		return
	}
	defer goodFile.Close()

	badFile, err := os.Create(badFile)
	if err != nil {
		fmt.Println("无法创建 bad.csv 文件:", err)
		return
	}
	defer badFile.Close()

	goodWriter := csv.NewWriter(goodFile)
	badWriter := csv.NewWriter(badFile)

	// 使用 bufio.Reader 读取文件
	reader := bufio.NewReaderSize(file, readBufferSize) // 使用 64KB 缓冲区
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxConcurrency) // 控制最大并发数

	client := createHttpClient() // 创建 HTTP 客户端

	// 读取表头，获取发布链接的列索引
	csvReader := csv.NewReader(reader)
	header, err := csvReader.Read()
	if err != nil {
		fmt.Println("无法读取文件表头:", err)
		return
	}

	// 确保表头中有"发布链接"这一列
	linkColumnIndex := -1
	for i, col := range header {
		if strings.TrimSpace(col) == "发布链接" {
			linkColumnIndex = i
			break
		}
	}

	if linkColumnIndex == -1 {
		fmt.Println("未找到发布链接列")
		return
	}

	// 打印进度
	n, err := countLines(inputFile)
	if err != nil {
		return
	}
	go func(count int) {
		defer fmt.Println("\n进度监控结束")       // 可选，退出时输出结束提示
		ticker := time.NewTicker(2 * time.Second) // 使用 ticker 更好地控制进度打印频率
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
			case <-ticker.C:
				progress := float64(lineCount.GetProcessed()) / float64(count) * 100
				fmt.Printf("\r进度：%.2f%% (%v/%v)\n", progress, lineCount.GetProcessed(), count)
			}
		}
	}(n)

	// 逐行读取 CSV 文件
	var goodRows, badRows [][]string

	for {
		record, err := reader.ReadString('\n') // 逐行读取
		if err != nil {
			break // 到达文件末尾
		}

		// 去掉换行符
		record = strings.TrimSpace(record)

		// 将一行记录解析为 CSV 格式
		recordFields := strings.Split(record, ",")
		if len(recordFields) < len(header) {
			continue // 跳过不完整的行
		}

		// 获取发布链接列的内容
		url := recordFields[linkColumnIndex]

		// 等待可用的信号量
		sem <- struct{}{}
		wg.Add(1)

		// 并发执行链接检查
		go func(record []string) {
			defer wg.Done()
			defer func() { <-sem }() // 释放信号量

			// 检查 URL 是否有效
			isValid := checkURL(ctx, client, url)

			// 将有效或无效的行存入相应的数组
			if isValid {
				goodRows = append(goodRows, record)
			} else {
				badRows = append(badRows, record)
			}
		}(recordFields)
	}

	// 等待所有 goroutines 完成
	wg.Wait()

	// 批量写入 CSV 文件
	writeCSVFile(goodWriter, goodRows)
	writeCSVFile(badWriter, badRows)

	fmt.Println("处理完毕，生成文件：good.csv 和 bad.csv")
}

func main() {
	// 创建一个带有全局超时的 context
	ctx, cancel := context.WithTimeout(context.Background(), globalTimeout)
	defer cancel()

	start := time.Now()
	processCSV(ctx)
	fmt.Printf("执行时间: %v\n", time.Since(start))
}
