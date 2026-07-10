import os
from s3_ingest import S3ClientWrapper

def main():
    BUCKET = "retailflow-bucket-2026"
    DATE_FOLDER = "dt=2026-07-08"
    
    files = ["orders_day1.json", "clickstream_day1.json", "order_items_day1.json"]
    s3 = S3ClientWrapper()
    
    for file in files:
        if not os.path.exists(file):
            print(f"Local file missing: {file}")
            continue
            
        # Dynamically extracts 'orders', 'clickstream', or 'order_items' by removing '_day1.json'
        folder_name = file.replace("_day1.json", "")
        
        # Output structure: raw/order_items/dt=2026-07-08/order_items_day1.json
        s3_path = f"raw/{folder_name}/{DATE_FOLDER}/{file}"
        
        print(f"Uploading {file}: {s3_path}")
        s3.upload_file(file, BUCKET, s3_path)
        print(f"Ingested successfully: {s3_path}\n")

if __name__ == "__main__":
    main()