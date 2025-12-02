
web: gunicorn fyers_algo.wsgi --log-file -

data_engine: python manage.py run_data_engine

algo_worker: python manage.py run_algo_worker

order_socket: python manage.py run_order_socket

scanner_worker: python manage.py run_scanner_worker
