from django.shortcuts import render
from django.core.paginator import Paginator, PageNotAnInteger, EmptyPage, InvalidPage
from django.http import HttpResponse
from .forms import SearchForm
import re
import os
import time

start = time.time()
def each_word():
    index = set()
    for filename in os.listdir("/home/uic/ws2_django_demo/dashboard/test/"):
        with open("/home/uic/ws2_django_demo/dashboard/test/"+filename, 'r',encoding= u'utf-8',errors='ignore') as f:
            i = 0
            article = f.read()
            article = ''.join(re.findall(r'[A-Za-z\s]',article))
            x = article.split()
            index = index.union(x)
    return index

def each_book():
    book = {}
    for filename in os.listdir("/home/uic/ws2_django_demo/dashboard/test/"):
        index = set()
        with open("/home/uic/ws2_django_demo/dashboard/test/"+filename, 'r',encoding= u'utf-8',errors='ignore') as f:
            i = 0
            file = os.path.basename(f.name)  # 当前文件名名称
            article = f.read()
            article = ''.join(re.findall(r'[A-Za-z\s]',article))
            x = article.split()
            index = index.union(x)
            book[file] = index
    return book

def index_result(index,book):    
    final_index={}
    for i in index:
        final_index[i] = []
        for i2 in book:
            if(i in book[i2]):
                final_index[i].append(i2)
    return final_index


import keyword
import linecache
import codecs
import math

def find_word(filename,word):
    path = "/home/uic/ws2_django_demo/dashboard/test/"+filename
    with codecs.open(path, 'r',encoding= u'utf-8',errors='ignore') as f:
        l = []
        i = 0
        word_amount = 0
        for line in f.readlines():
            i = i+1
            line = ''.join(re.findall(r'[A-Za-z\s]',line))
            if (line.find(word) != -1):
                l.append(i) 
        the_line = linecache.getline(path,l[0] )     
    return the_line[:-1],l,word_amount


index = each_word()
book = each_book()
allresults = index_result(index,book)

def find_book(word):
    final_index = allresults 
    appear_book = final_index[word]
    total_book = len(appear_book)
    x = []
    y = []
    z = []
    for filename in appear_book:
        with codecs.open("/home/uic/ws2_django_demo/dashboard/test/"+filename, 'r',encoding= u'utf-8',errors='ignore') as f:
            contents = f.read()
            words = contents.rstrip()
            num_words = len(words)
            a,b,c = find_word(filename,word)
            tf = c/num_words
            idf = math.log(1134/(total_book+1))
            tf_idf = tf*idf
            x.append(a)
            y.append(b)
            z.append(tf_idf)
    return x,y,appear_book,z


def multipule_word(word):
    wordlist = word.split()
    intersection_appearbook = set()
    first_word = check(wordlist[0])
    intersection_appearbook  = set(allresults[first_word])
    for i in wordlist:
        if i in index: 
            x = allresults[i]
        else:
            real_word = check(i)
            x = allresults[real_word]
        intersection_appearbook = intersection_appearbook.intersection(x)
    both_book = list(intersection_appearbook)
    x = []
    y = []
    for filename in both_book:
        with codecs.open("/home/uic/ws2_django_demo/dashboard/test/"+filename, 'r',encoding= u'utf-8',errors='ignore') as f:
            a,b = find_word2(filename,wordlist[0])
            x.append(a)
            y.append(b)
    return x,y,both_book

def result_show2(word):
    x,y,z = multipule_word(word)
    x = handle_result(x)
    return x,y,z

def find_word2(filename,word):
    path = "/home/uic/ws2_django_demo/dashboard/test/"+filename
    with codecs.open(path, 'r',encoding= u'utf-8',errors='ignore') as f:
        l = []
        i = 0
        for line in f.readlines():
            i = i+1
            line = ''.join(re.findall(r'[A-Za-z\s]',line))
            if (line.find(word) != -1):
                l.append(i)
        the_line = linecache.getline(path,l[0] )     
    return the_line[:-1],l


def handle_result(word_position):
    length = len(word_position)
    for i in range(length):
        x =  word_position[i]
        if len(x)>100:
            x = x[:150]+'...'
            word_position[i] = x
    return word_position


# In[22]:


def result_show(word):
    x,y,z,w = find_book(word)
    x = handle_result(x)
    return x,y,z,w


def final_dict(word):
    all_list = []
    pra,position,book,tfidf = result_show(word)
    x = len(pra)
    for i in range(x):
        dict1 = {}
        dict1['book_name'] = book[i]
        dict1['line'] = pra[i]   
        dict1['position'] = (position[i])[0] 
        dict1['TF_IDF'] = tfidf[i]
        all_list.append(dict1)
    all_list = sorted(all_list,key=lambda keys:keys['TF_IDF'])
    return all_list

def mul_final_dict(word):
    all_list=[]
    pra,position,book= result_show2(word)
    x = len(pra)
    for i in range(x):
        dict1 = {}
        dict1['book_name'] = book[i]
        dict1['line'] = pra[i]
        dict1['position'] = (position[i])[0]
        all_list.append(dict1)
    return all_list

from collections import Counter

def check(word):

    fre = dict(Counter(word))

    similars = {w:[fre[ch]-words[w].get(ch,0) for ch in word]+[words[w][ch]-fre.get(ch,0) for ch in w] for w in words}

    return min(similars.items(), key = lambda item:sum(map(lambda i:i**2, item[1])))[0]


words = {word:dict(Counter(word)) for word in index}

def search(request):
    context = dict()
    if request.method == "POST":
        form = SearchForm(request.POST)
        if form.is_valid():
            post_data = form.cleaned_data['search']
        if ' ' not in post_data:
            if post_data in index: 
                result = final_dict(post_data)
            else:
                right_word = check(post_data)
                result = final_dict(right_word)
            number = len(result)
            end = time.time()
            cost = (end - start)/1000
            context['filename'] = result
            context['number'] = number
            context['time'] = cost
            context['form'] = form
        else:
            result = mul_final_dict(post_data)
            number = len(result)
            end = time.time()
            cost = (end - start)/100
            context['filename'] = result
            context['number'] = number
            context['time'] = cost
            context['form'] = form
        return render(request, 'results.html', context)
    else:
        form = SearchForm()
        context['form'] = form
        return render(request, 'index.html', context)
    

def results(request):
    q = request.GET.get('q')
    context = dict()
    if request.method == "POST":
        form = SearchForm(request.POST)
        if form.is_valid():
            post_data = form.cleaned_data['search']
        result = final_dict(post_data)
        number = len(result)
        context['filename'] = result
        context['number'] = number
        context['time'] = cost
        context['form'] = form
        return render(request, 'results.html', context)
    else:
        form = SearchForm()
        context['form'] = form
        return render(request, 'index.html', context)
