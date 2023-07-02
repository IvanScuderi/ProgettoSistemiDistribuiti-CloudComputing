import backend.main.python.connection.livy_client as livy
from django.http import HttpResponseRedirect
from django.shortcuts import render
import sys
import os
sys.path.insert(0, 'C:\\Users\\1997i\\PycharmProjects\\pythonProject')


client = livy.create_client()
df = None
session_id = None


def redirect(request):
    if os.path.exists(r'C:\Users\1997i\livy-spark-cluster\frontend\static\img\occurrence.jpg'):
        os.remove(r'C:\Users\1997i\livy-spark-cluster\frontend\static\img\occurrence.jpg')
    if os.path.exists(r'C:\Users\1997i\livy-spark-cluster\frontend\static\img\mean_views.jpg'):
        os.remove(r'C:\Users\1997i\livy-spark-cluster\frontend\static\img\mean_views.jpg')
    if os.path.exists(r'C:\Users\1997i\livy-spark-cluster\frontend\static\img\owner_overview_1.jpg'):
        os.remove(r'C:\Users\1997i\livy-spark-cluster\frontend\static\img\owner_overview_1.jpg')
    if os.path.exists(r'C:\Users\1997i\livy-spark-cluster\frontend\static\img\owner_overview_2.jpg'):
        os.remove(r'C:\Users\1997i\livy-spark-cluster\frontend\static\img\owner_overview_2.jpg')
    return HttpResponseRedirect('/progettosistdist/')


def home(request):
    global client
    global df
    global session_id
    model = {'caricato': False,
             'home': True
             }
    if request.method == 'POST':
        df = livy.df
        session_id = livy.create_session(client)
        model['righe'] = df.shape[0]
        model['colonne'] = df.shape[1]
        model['caricato'] = True
        model['iter'] = df.iloc[0:20].iterrows()
    if df is None:
        model['caricato'] = False
    else:
        model['caricato'] = True
        model['righe'] = df.shape[0]
        model['colonne'] = df.shape[1]
        model['iter'] = df.iloc[0:20].iterrows()
    return render(request, 'home.html', model)


def clear(request):
    global client
    global session_id
    global valutato
    global df
    df = None
    valutato = False
    model = {'caricato': False,
             'home': True
             }
    livy.delete_session(client, session_id)
    if os.path.exists(r'C:\Users\1997i\livy-spark-cluster\frontend\static\img\occurrence.jpg'):
        os.remove(r'C:\Users\1997i\livy-spark-cluster\frontend\static\img\occurrence.jpg')
    if os.path.exists(r'C:\Users\1997i\livy-spark-cluster\frontend\static\img\mean_views.jpg'):
        os.remove(r'C:\Users\1997i\livy-spark-cluster\frontend\static\img\mean_views.jpg')
    if os.path.exists(r'C:\Users\1997i\livy-spark-cluster\frontend\static\img\owner_overview_1.jpg'):
        os.remove(r'C:\Users\1997i\livy-spark-cluster\frontend\static\img\owner_overview_1.jpg')
    if os.path.exists(r'C:\Users\1997i\livy-spark-cluster\frontend\static\img\owner_overview_2.jpg'):
        os.remove(r'C:\Users\1997i\livy-spark-cluster\frontend\static\img\owner_overview_2.jpg')
    return render(request, 'home.html', model)


def error(request):
    model = {}
    model['titolo'] = "Ops! Si è verificato un errore, " \
                      "Controlla se la sessione livy sul cluster è stata inizializzata con successo."
    return render(request, 'error.html', model)


def plot(request):
    global df
    model = {'caricato': True,
             'completato': False,
             'nElem': df.shape[0]
             }
    if request.method == 'POST':
        elem = request.POST.get('elem')
        try:
            elem = int(elem)
        except ValueError:
            model['titolo'] = "Ops! Si è verificato un errore, prova a controllare i valori in input alla query..."
            return render(request, 'error.html', model)
        if elem < 0:
            model['titolo'] = "Ops! Si è verificato un errore, prova a controllare i valori in input alla query..."
            return render(request, 'error.html', model)
        st, n = livy.statement_lat_lon()
        ris = livy.submit_statement(client, session_id, st, n)
        livy.show_lat_lon_maps(ris, elem)
        model['completato'] = True
        model['p'] = elem
    return render(request, 'plot.html', model)


def directions(request):
    global df
    model = {'caricato': True,
             'completato': False,
             'users': len(df['owner'].unique())
             }
    if request.method == 'POST':
        utenti = request.POST.get('utenti')
        npost = request.POST.get('npost')
        try:
            utenti = int(utenti)
            npost = int(npost)
        except ValueError:
            model['titolo'] = "Ops! Si è verificato un errore, prova a controllare i valori in input alla query..."
            return render(request, 'error.html', model)
        if (utenti < 0) or (npost < 0):
            model['titolo'] = "Ops! Si è verificato un errore, prova a controllare i valori in input alla query..."
            return render(request, 'error.html', model)
        st, n = livy.statement_directions(utenti, npost)
        ris = livy.submit_statement(client, session_id, st, n)
        livy.show_directions_maps(ris)
        model['completato'] = True
        model['utenti'] = utenti
        model['post'] = npost
    return render(request, 'directions.html', model)


def kmeans(request):
    global df
    model = {'caricato': True,
             'completato': False
             }
    if request.method == 'POST':
        k = request.POST.get('k')
        hm = request.POST.get('hm')
        try:
            k = int(k)
            hm = int(hm)
        except ValueError:
            model['titolo'] = "Ops! Si è verificato un errore, prova a controllare i valori in input alla query..."
            return render(request, 'error.html', model)
        if (k < 0) or (hm < 0):
            model['titolo'] = "Ops! Si è verificato un errore, prova a controllare i valori in input alla query..."
            return render(request, 'error.html', model)
        st, n = livy.statement_kmeans(k)
        pred, center_eval = livy.submit_statement(client, session_id, st, n)
        livy.show_kmeans_map(pred, center_eval, k, hm)
        # silhouette coefficient
        sc = center_eval.iloc[k]['evaluation']
        model['sc'] = sc
        model['k'] = k
        model['cluster'] = True
        model['completato'] = True
        model['p'] = hm
    return render(request, 'kmeans.html', model)


def tag(request):
    if os.path.exists(r'C:\Users\1997i\PycharmProjects\frontend\static\img\occurrence.jpg'):
        os.remove(r'C:\Users\1997i\PycharmProjects\frontend\static\img\occurrence.jpg')
    if os.path.exists(r'C:\Users\1997i\PycharmProjects\frontend\static\img\mean_views.jpg'):
        os.remove(r'C:\Users\1997i\PycharmProjects\frontend\static\img\mean_views.jpg')
    global df
    model = {'caricato': True,
             'completato': False,
             'mostra_tag': False
             }
    if request.method == 'POST':
        e = request.POST.get('escludi')
        escludi = False
        num_esclusi = 0
        ntag = request.POST.get('tag')
        ordinamento = request.POST.get('ordinamento')
        order = False
        O = "Occorrenze"
        E = "No"
        try:
            ntag = int(ntag)
        except ValueError:
            model['titolo'] = "Ops! Si è verificato un errore, prova a controllare i valori in input alla query..."
            return render(request, 'error.html', model)
        if ntag < 0:
            model['titolo'] = "Ops! Si è verificato un errore, prova a controllare i valori in input alla query..."
            return render(request, 'error.html', model)
        if e == "m1":
            escludi = True
            num_esclusi = 1
            E = "Si [ < 1 ]"
        if e == "m2":
            escludi = True
            num_esclusi = 2
            E = "Si [ < 2 ]"
        if e == "m3":
            escludi = True
            num_esclusi = 3
            E = "Si [ < 3 ]"
        if ordinamento == "views":
            order = True
            O = "Visualizzazioni"
        st, n = livy.statement_tags(num_esclusi=num_esclusi, n=ntag, order_by_mean=order, escludi=escludi)
        ris = livy.submit_statement(client, session_id, st, n)
        livy.save_plot_tags(ris)
        model['completato'] = True
        model['mostra_tag'] = True
        model['N'] = ntag
        model['O'] = O
        model['E'] = E
    return render(request, 'tag.html', model)


def active_users(request):
    if os.path.exists(r'C:\Users\1997i\PycharmProjects\frontend\static\img\owner_overview_2.jpg'):
        os.remove(r'C:\Users\1997i\PycharmProjects\frontend\static\img\owner_overview_2.jpg')
    if os.path.exists(r'C:\Users\1997i\PycharmProjects\frontend\static\img\owner_overview_1.jpg'):
        os.remove(r'C:\Users\1997i\PycharmProjects\frontend\static\img\owner_overview_1.jpg')
    global df
    model = {'caricato': True,
             'completato': False,
             'users': len(df['owner'].unique())
             }
    if request.method == "POST":
        utenti = request.POST.get('utenti')
        try:
            utenti = int(utenti)
        except ValueError:
            model['titolo'] = "Ops! Si è verificato un errore, prova a controllare i valori in input alla query..."
            return render(request, 'error.html', model)
        if utenti < 0:
            model['titolo'] = "Ops! Si è verificato un errore, prova a controllare i valori in input alla query..."
            return render(request, 'error.html', model)
        st, n = livy.statement_owner(utenti)
        ris = livy.submit_statement(client, session_id, st, n)
        livy.save_plot_owner(ris)
        model['num'] = utenti
        model['iter1'] = ris.iterrows()
        model['iter2'] = ris.iterrows()
        model['completato'] = True
    return render(request, 'active_users.html', model)







