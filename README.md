# **Realtime Streaming Coinmarketcap Data Pipeline**

## **Mô tả dự án**

Dự án này xây dựng một **Data Pipeline Streaming** từ **Coinmarketcap** để thu thập dữ liệu thời gian thực, xử lý với **Apache Spark**, và lưu trữ trên **Amazon S3**. Sau đó, dữ liệu được phân tích với **Amazon Athena**, với metadata được quản lý bởi **AWS Glue**.

---

## **Kiến trúc tổng quan**

Dưới đây là kiến trúc của hệ thống:
![Architecture Diagram](https://github.com/user-attachments/assets/63f008e8-c4b0-4418-9762-f6390200ff82)

---

## **Các thành phần chính**

### **1. Web Scraping với Scrapy**
- **Scrapy** được sử dụng để thu thập dữ liệu từ **Coinmarketcap.com**.
- Dữ liệu bao gồm các trường: `Rank`, `Name`, `Symbol`, `Price`, `Market Cap`, `Volume`, `Change (1h)`, `Change (24h)`, `Change (7d)`, `uuid`.

### **2. Apache Kafka**
- **Kafka** làm hệ thống message queue để stream dữ liệu từ Scrapy.
- **Apache Zookeeper** được sử dụng để quản lý các Kafka broker.

### **3. Apache Spark Streaming**
- Xử lý dữ liệu thời gian thực từ **Kafka** với Spark Streaming.
- Dữ liệu được làm sạch và chuyển đổi:
  - Loại bỏ các ký tự đặc biệt trong các trường số liệu (`Price`, `Market Cap`, `Volume`).
  - Thêm cột `dates` và `hours` từ UUID để phục vụ phân tích thời gian.
  - Bổ sung UUID để identify dữ liệu theo thời gian, lấy thời gian làm ID.

### **4. Amazon S3**
- Dữ liệu sau khi được xử lý sẽ được lưu trữ dưới dạng **JSON** trong **Amazon S3**.
![Processed Data](https://github.com/user-attachments/assets/53e111ae-87ec-4bac-938c-48bc78f2779f)

---

### **5. AWS Glue và Data Catalog**
- **Crawler** của Glue quét dữ liệu từ S3 để phát hiện schema.
- Metadata của dữ liệu được lưu trữ trong **AWS Glue Data Catalog**.
![AWS Glue Metadata](https://github.com/user-attachments/assets/769782fc-ac42-413f-9590-f7cccabc89ba)

---

### **6. Amazon Athena**
- Sử dụng Athena để chạy các truy vấn SQL trực tiếp trên dữ liệu đã lưu trữ trong S3.
![Athena Query Result](https://github.com/user-attachments/assets/bd1d1ca8-2b0b-4a24-bd13-2efa5fb8711a)

---

## **Công nghệ sử dụng**

| Công nghệ           | Mô tả                                                         |
|---------------------|---------------------------------------------------------------|
| **Scrapy**          | Thu thập dữ liệu từ Coinmarketcap                              |
| **Apache Kafka**    | Streaming dữ liệu từ Scrapy                                   |
| **Apache Zookeeper**| Quản lý Kafka Broker                                          |
| **Apache Spark**    | Xử lý dữ liệu thời gian thực từ Kafka                         |
| **AWS Glue**        | Tự động phát hiện schema và quản lý metadata                  |
| **Amazon S3**       | Lưu trữ dữ liệu thô và dữ liệu đã xử lý                       |
| **Amazon Athena**   | Truy vấn dữ liệu trực tiếp từ S3                              |

---

## **Thư mục dự án**

```plaintext
├── result/
│   ├── architecture-diagram.png
│   ├── processed-data.png
│   ├── athena-query-result.png
├── coin_data/
│   ├── __init__.py
│   ├── items.py
│   ├── middlewares.py
│   ├── pipelines.py
│   ├── settings.py
│   └── spiders/
│       ├── __init__.py
│       └── coin.py
├── scrapy.cfg
├── jobs/
│   ├── consumer.py
│   ├── config.py
├── docker-compose.yml
├── producer.py
├── test.py
├── requirements.txt
├── README.md
