from django.contrib import admin
from django.urls import path, include
from progettosistdist import views

urlpatterns = [
    path('', views.redirect),
    path('progettosistdist/', views.home),
    path('plot/', views.plot),
    path('directions/', views.directions),
    path('kmeans/', views.kmeans),
    path('tag/', views.tag),
    path('active_users/', views.active_users),
    path('clear/', views.clear),
    path('error/', views.error),
    path('admin/', admin.site.urls),
]
