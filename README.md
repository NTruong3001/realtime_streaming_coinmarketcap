# realtime_streaming_coinmarketcap
# Coinmarketcap Streaming Data Pipeline

## Mô tả dự án

Dự án này xây dựng một **Data Pipeline Streaming** từ **Coinmarketcap** để thu thập dữ liệu thời gian thực, xử lý dữ liệu với **Apache Spark**, và lưu trữ dữ liệu trên **Amazon S3**. Sau đó, dữ liệu được phân tích với **Amazon Athena**, cùng với việc sử dụng **AWS Glue** để quản lý metadata.

---

## Kiến trúc tổng quan

Dưới đây là kiến trúc của hệ thống:

![Architecture Diagram]
![abc](https://github.com/user-attachments/assets/90ac99a3-9b2e-4d11-ac20-7a2cbab535b2)

---

## Các thành phần chính

### 1. **Web Scraping với Scrapy**
- Sử dụng **Scrapy** để thu thập dữ liệu từ **Coinmarketcap.com**.
- Dữ liệu bao gồm các trường: `Rank`, `Name`, `Symbol`, `Price`, `Market Cap`, `Volume`, `Change (1h)`, `Change (24h)`, `Change (7d)`, `uuid`.

### 2. **Apache Kafka**
- Sử dụng **Kafka** làm hệ thống message queue để stream dữ liệu từ **Scrapy**.
- **Apache Zookeeper** được sử dụng để quản lý các Kafka broker.

### 3. **Apache Spark Streaming**
- Xử lý dữ liệu thời gian thực từ **Kafka** với Spark Streaming.
- Dữ liệu được làm sạch và chuyển đổi, ví dụ:
  - Loại bỏ các ký tự đặc biệt trong các trường số liệu (`Price`, `Market Cap`, `Volume`).
  - Thêm cột `dates` và `hours` từ UUID để phục vụ phân tích thời gian.

### 4. **Amazon S3**
- Dữ liệu sau khi được xử lý sẽ được lưu trữ dưới dạng **JSON** trong **Amazon S3**.

### 5. **AWS Glue và Data Catalog**
- **Crawler** của Glue quét dữ liệu từ S3 để phát hiện schema.
- Metadata của dữ liệu được lưu trữ trong **AWS Glue Data Catalog**.

### 6. **Amazon Athena**
- Sử dụng Athena để chạy các truy vấn SQL trực tiếp trên dữ liệu đã lưu trữ trong S3.

---

## Công nghệ sử dụng

| Công nghệ      | Mô tả                                                         |
|----------------|---------------------------------------------------------------|
| **Scrapy**     | Thu thập dữ liệu từ Coinmarketcap                              |
| **Apache Kafka** | Streaming dữ liệu từ Scrapy                                   |
| **Apache Zookeeper** | Quản lý Kafka Broker                                      |
| **Apache Spark** | Xử lý dữ liệu thời gian thực từ Kafka                        |
| **AWS Glue**    | Tự động phát hiện schema và quản lý metadata                  |
| **Amazon S3**   | Lưu trữ dữ liệu thô và dữ liệu đã xử lý                       |
| **Amazon Athena** | Truy vấn dữ liệu trực tiếp từ S3                            |

---

## Kết quả

Dưới đây là ảnh minh họa kết quả của pipeline:

### Kết quả xử lý dữ liệu
![Processed Data](https://raw.githubusercontent.com/<your-username>/<your-repo>/main/images/processed-data.png)

### Truy vấn trên Amazon Athena
![Athena Query Result](https://raw.githubusercontent.com/<your-username>/<your-repo>/main/images/athena-query-result.png)

---

## Hướng dẫn triển khai

### 1. Cấu hình môi trường
- Cài đặt Docker, Python, và Apache Kafka.
- Đảm bảo cấu hình quyền truy cập **AWS IAM** để sử dụng Amazon S3, Glue, và Athena.

### 2. Chạy Scrapy
- Tạo Scrapy Spider để thu thập dữ liệu từ **Coinmarketcap.com**:
  ```bash
  scrapy crawl coinmarketcap

