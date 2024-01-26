# ODAP
Đồ án thực hành ODAP
## Mô tả: 
Đồ án giả sử việc xử lý dữ liệu trực tuyến bằng cách đọc dữ liệu từ tập tin .csv và xem từng dòng dữ liệu như là dữ liệu giao dịch đang phát sinh trong thực tế. Sử dụng Kafka để đọc các dòng dữ liệu và thông qua Spark (pyspark) để lư trữ dữ liệu lên HDFS dưới dạng các file csv. Sau đó sử dụng Power BI để trực quan hóa dữ liệu đã lưu trữ.
## Yêu cầu:
- Kafka phiên bản 3.6.0 (hoặc tương tự)
- Hadoop phiên bản 3.3.6 (hoặc tương tự)
- Tải tập dữ liệu mẫu [tại đây](https://studenthcmusedu-my.sharepoint.com/:f:/g/personal/20120255_student_hcmus_edu_vn/EjyCo3AI5TRIt-Z4qnnL07oBMVdvnCPyglWkS9qQZCmb8A?e=4twKS1) 

## Cách run chương trình
#### Bước 1: Run Kafka theo các bước trong file txt.txt
#### Bước 2: Run file 1.py
#### Bước 3: Run file 2.py bằng Spark

## Lưu ý
*Kiểm tra mức độ tương thích của các đường dẫn file*
