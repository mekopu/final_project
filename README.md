# final_project

Pada final project ini bertujuan untuk mengetahui total kasus harian covid di Provinsi Jawa Barat berdasarkan total provinsi dan masing-masing kabupaten. Hasil data akan diperbarui setiap harinya pada pukul 00:00:00 dengan scheduler pada Airflow. Data didapatkan dari public API berikut. 
```
curl -X GET "http://103.150.197.96:5001/api/v1/rekapitulasi_v2/jabar/harian?level=kab"
-H "accept: application/json"
```

Data staging akan disimpan terlebih dahulu pada MySQL yang kemudian akan diagregasi berdasarkan tanggal, kabupaten, dan statusnya. Hasil akhir akan berupa table district_daily dan province_daily pada PostgreSQL.
