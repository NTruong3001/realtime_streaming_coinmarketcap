
# **Realtime Streaming Coinmarketcap Data Pipeline**

## **Mô tả dự án**

Dự án này xây dựng một **Data Pipeline Streaming** từ **Coinmarketcap** để thu thập dữ liệu thời gian thực, xử lý với **Apache Spark**, và lưu trữ trên **Amazon S3**. Sau đó, dữ liệu được phân tích với **Amazon Athena**, với metadata được quản lý bởi **AWS Glue**.

---

## **Kiến trúc tổng quan**

Dưới đây là kiến trúc của hệ thống:

![Architecture Diagram](https://github.com/user-attachments/assets/eb7782cb-218a-41e9-a02a-9364ca4cc9f0)

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

## **Kết quả**

### **Kết quả xử lý dữ liệu**
![Processed Data](https://github.com/user-attachments/assets/00929068-b687-4827-9a96-6eff2bebf847)

### **Truy vấn trên Amazon Athena**
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
```

---

## **Hướng dẫn triển khai**

### **1. Cấu hình môi trường**
- Đảm bảo bạn đã cài đặt:
  - **Docker**
  - **Python** (phiên bản >= 3.6)
  - **Apache Kafka**
- Cài đặt các thư viện cần thiết:
  ```bash
  pip install -r requirements.txt
  ```
- Đảm bảo cấu hình quyền truy cập **AWS IAM** để sử dụng Amazon S3, Glue, và Athena.

---

### **2. Khởi động Kafka và Zookeeper bằng Docker**
- Chạy lệnh sau để khởi động Kafka và Zookeeper:
  ```bash
  docker-compose up -d
  ```

---

### **3. Chạy Scrapy**
- Tạo Scrapy Spider để thu thập dữ liệu từ **Coinmarketcap.com**:
  ```bash
  scrapy startproject coin_data
  ```

- Sau đó, trong thư mục `coin_data`, bạn sẽ có cấu trúc như sau:
  ```plaintext
  coin_data/
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
  ```

- Chạy Scrapy để thu thập dữ liệu:
  ```bash
  scrapy crawl coin
  ```

---

### **4. Xử lý dữ liệu với Spark**
- Chạy mã để đọc dữ liệu được crawl bằng:
  ```bash
  python producer.py
  ```

- Sau đó, submit `consumer.py` lên Spark để xử lý dữ liệu và upload lên Amazon S3:
  ```bash
  docker exec -it realtime_streaming_coinmarketcap-spark-master-1 spark-submit   --master spark://spark-master:7077   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk:1.11.469   /opt/bitnami/spark/jobs/consumer.py
  ```



## **Liên hệ**
Nếu có bất kỳ câu hỏi hoặc đóng góp nào, vui lòng liên hệ:

- **Name**:Ngyễn Nhật Trường
- **Email**:truongnn30012gmail.com
- **GitHub**: https://github.com/NTruong3001
