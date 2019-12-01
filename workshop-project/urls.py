from django.urls import path
from . import views


urlpatterns = [
    path('index', views.search, name='index'),
    path('index.html',views.search,name='index')
]