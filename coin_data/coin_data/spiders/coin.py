import scrapy
from scrapy_splash import SplashRequest
import json
from datetime import datetime
import uuid
import os
class CoinMarketCapSpider(scrapy.Spider):
    name = "coin"
    start_urls = ["https://coinmarketcap.com/all/views/all/"]

    # Định nghĩa script Lua cho Splash để mô phỏng cuộn trang
    lua_script = """
    function main(splash)
        splash:go(splash.args.url)
        splash:wait(3)  -- Tăng thời gian chờ để trang tải
        -- Mô phỏng cuộn trang xuống cuối trang
        splash:runjs("window.scrollTo(0, document.body.scrollHeight)")  
        splash:wait(3)  -- Chờ thêm một chút để đảm bảo trang đã tải hết
        return splash:html()
    end
    """

    # Biến đếm số lượng coin đã thu thập
    coin_count = 0

    # Tạo tên file dựa trên timestamp
    def get_file_path(self):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = f'E:/DE/final_project/realtime_streaming_coinmarketcap/data/{timestamp}.json'
        os.makedirs(os.path.dirname(file_path), exist_ok=True)  # Tạo thư mục nếu chưa có
        return file_path

    def start_requests(self):
        for url in self.start_urls:
            yield SplashRequest(url, self.parse, endpoint='execute', args={'lua_source': self.lua_script})

    def parse(self, response):
        # In nội dung HTML để kiểm tra
        self.logger.info("HTML của trang: " + response.text[:500])  # In 500 ký tự đầu tiên của HTML

        # Lấy các hàng dữ liệu trong bảng
        rows = response.css('tr.cmc-table-row')
        data = []  # Danh sách lưu trữ dữ liệu thu thập được

        for row in rows:
            if self.coin_count >= 20:
                break  # Dừng spider khi đã thu thập đủ 20 coin

            coin_data = {
                'Rank': row.css('td.cmc-table__cell--sort-by__rank div::text').get(default='N/A'),
                'Name': row.css('td.cmc-table__cell--sort-by__name a.cmc-link::text').get(default='N/A'),
                'Symbol': row.css('td.cmc-table__cell--sort-by__symbol div::text').get(default='N/A'),
                'Price': row.css('td.cmc-table__cell--sort-by__price span::text').get(default='N/A'),
                'Market Cap': row.css('td.cmc-table__cell--sort-by__market-cap span.sc-11478e5d-0.chpohi::text').get(default='N/A'),
                'Volume': row.css('td.cmc-table__cell--sort-by__volume-24-h a.cmc-link::text').get(default='N/A'),
                'Change (1h)': row.css('td.cmc-table__cell--sort-by__percent-change-1-h div::text').get(default='N/A'),
                'Change (24h)': row.css('td.cmc-table__cell--sort-by__percent-change-24-h div::text').get(default='N/A'),
                'Change (7d)': row.css('td.cmc-table__cell--sort-by__percent-change-7-d div::text').get(default='N/A'),
                'uuid': str(uuid.uuid1()) 
            }

            data.append(coin_data)  # Thêm dữ liệu vào danh sách
            self.coin_count += 1  # Tăng biến đếm lên sau khi thu thập một coin

        # Sau khi thu thập xong, lưu dữ liệu vào file JSON
        if data:
            file_path = self.get_file_path()
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=4)
            self.logger.info(f"Dữ liệu đã được lưu vào: {file_path}")

        # Kiểm tra nếu có trang tiếp theo để tải
        if self.coin_count < 21:
            next_button = response.css('li.pagination__item--next a::attr(href)').get()
            if next_button:
                next_url = response.urljoin(next_button)
                self.logger.info(f"Đi tới trang tiếp theo: {next_url}")
                yield SplashRequest(next_url, self.parse, endpoint='execute', args={'lua_source': self.lua_script})
            else:
                self.logger.info("Không còn trang tiếp theo.")
        else:
            self.logger.info("Đã thu thập đủ 20 đồng coin.")
