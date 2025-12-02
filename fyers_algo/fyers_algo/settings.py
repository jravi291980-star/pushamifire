import os
import environ
from pathlib import Path

# Initialize environment variables
env = environ.Env(
    # set casting and default value
    DEBUG=(bool, False),
    REDIS_URL=(str, 'redis://localhost:6379/0'),
    FYERS_APP_ID=(str, 'YOUR_FYERS_APP_ID'),
    FYERS_SECRET_KEY=(str, 'YOUR_FYERS_SECRET_KEY'),
    FYERS_CALLBACK_URL=(str, 'http://127.0.0.1:8000/trading/auth/fyers/callback/')
)

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# Read .env file for local development
environ.Env.read_env(os.path.join(BASE_DIR, '.env'))

SECRET_KEY = env('SECRET_KEY', default='django-insecure-default-key-for-local-dev')
DEBUG = env('DEBUG')

ALLOWED_HOSTS = ['*'] # Be specific in production!

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'trading',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'whitenoise.middleware.WhiteNoiseMiddleware', # For Heroku static files
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'fyers_algo.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'fyers_algo.wsgi.application'

DATABASES = {
    'default': env.db_url(default='sqlite:///db.sqlite3')
}

AUTH_PASSWORD_VALIDATORS = [
    {'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator'},
    {'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator'},
    {'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator'},
    {'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator'},
]

LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_TZ = True

STATIC_URL = 'static/'
STATIC_ROOT = BASE_DIR / 'staticfiles'
STATICFILES_STORAGE = 'whitenoise.storage.CompressedManifestStaticFilesStorage'

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# Custom Configuration
LOGIN_REDIRECT_URL = '/trading/dashboard/'
LOGOUT_REDIRECT_URL = '/login/'

# FYERS/REDIS CONFIG (Read from environment variables)
FYERS_APP_ID = env('FYERS_APP_ID')
FYERS_SECRET_KEY = env('FYERS_SECRET_KEY')
FYERS_CALLBACK_URL = env('FYERS_CALLBACK_URL')
REDIS_URL = env('REDIS_URL')