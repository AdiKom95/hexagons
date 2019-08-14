# -*- coding: utf-8 -*-
"""
Created on Wed Jul 10 13:30:21 2019

@author: Adi
"""



### Biblioteki

import psycopg2


### Zmienne globalne

#Paramtery do polaczenia z baza danych 
nazwa_bazy = '******'
uzytkownik = '******' 
haslo = '*****' 



################################ CZESC I - SIATKA HEKSAGONALNA ##################################


def siatka_heksagonalna(srid = 4326, offset_y = 0.1, warstwa_siatki = 'hex_grid_n', warstwa_zasiegu = 'baea_nests'):
        
    """
    
    Funkcja pobiera od uzytkownika parametry takie jak: uklad wspolrzednych, dlugosc boku heksagonu - offeet_y, nazwe warstwy, do ktorej zasiegu ma zostac wyrysowana siatka heksagonow oraz nazwe siatki szesciokatow foremnych.
    """
    ### Nawiazywanie polaczenia z baza 
        
    try:
        
        conn = psycopg2.connect(host="localhost",database=nazwa_bazy, user=uzytkownik, password=haslo)
        print('OK')
    
    except:
        print('Cos poszlo nie tak...')
        
        
    ### Utworzenie kursora
        
    cur = conn.cursor()
    
    
    
    ## Zapytanie - zasiegi
    
    query3 = """select min(ST_XMin(geom)) as minX , max(ST_XMax(geom)) as maxX, min(ST_YMin(geom)) as minY, max(ST_YMax(geom)) as maxY from {warstwa_zasiegu};""".format(warstwa_zasiegu = warstwa_zasiegu)
    
    
    cur.execute(query3)
    b = cur.fetchone()
    
    minX,maxX,minY,maxY= list(b)
    
    
    
    offset_x = offset_y*pow(3,1/2)/2  # offset_x - wysokosc trojkata rownobocznego 
    
    offset2_x = offset_x*2  # prawdziwe przesuniecie, roznica miedzy heksagonami w poziomie 
    
    
    ldX,ldY = minX - offset_x, minY-0.5*offset_y
    lgX,lgY = minX- offset_x, minY+0.5*offset_y
    sgX, sgY = minX, minY + offset_y
    pgX,pgY =  minX + offset_x, minY+0.5*offset_y
    pdX,pdY =  minX + offset_x, minY-0.5*offset_y
    sdX, sdY = minX, minY - offset_y
    
    
    offset2_y = offset_y*3  # prawdziwe przesuniecie - roznica miedzy heksagonami w pionie
    
    
    ## Obliczenie wartosci maksymalnej dla funkcji SQL - generate_series
    
    zasiegX = abs(maxX - minX) # roznica max - min w poziomie - zasieg warstwy 
    offsety_w_x = int(round(zasiegX/offset2_x + 1,0)) # ilosc szesciokatow foremnych, ktore powinny pokrywac warstwe w poziomie / 1 naddatku
    ilosc_x = offsety_w_x * offset2_x # obliczenie zasiegu granicznego dla szesciokatow - konieczne dla funkcji SQL
    
    zasiegY = abs(maxY - minY) # kroki analogiczne dla pokrycia w pionie 
    offsety_w_y = int(round(zasiegY/offset2_y + 1,0))
    ilosc_y = offsety_w_y * offset2_y 
    
    
    ## Zapytanie tworzace relacje w bazie przechowujaca szesciokaty foremne
    
    query2= """
    create TABLE {warstwa_siatki} (gid serial not null primary key);
    SELECT addgeometrycolumn('{warstwa_siatki}','geom', {srid}, 'POLYGON', 2);
    
    INSERT INTO {warstwa_siatki} (geom)
    SELECT st_translate(geom, x_series, y_series)
    from generate_series(0, {ilosc_x}, {roznica_x}) AS x_series,
    generate_series(0, {ilosc_y}, {roznica_y}) as y_series,
    
    (
       SELECT ST_setSRID('POLYGON(({ldX} {ldY},{lgX} {lgY},{sgX} {sgY},{pgX} {pgY},{pdX} {pdY},{sdX} {sdY},{ldX} {ldY}))'::geometry,{srid}) as geom
       UNION
       SELECT ST_Translate(st_setSRID('POLYGON(({ldX} {ldY},{lgX} {lgY},{sgX} {sgY},{pgX} {pgY},{pdX} {pdY},{sdX} {sdY},{ldX} {ldY}))'::geometry,{srid}), {offset_x}, {offset2_y})  as geom
    
    ) as two_hex;
    
    """.format(srid=srid, minX = minX, maxX=maxX, minY=minY, maxY=maxY,ldX=ldX, ldY=ldY, lgX=lgX, lgY=lgY, sgX=sgX, sgY=sgY, pgX=pgX, pgY=pgY, pdX=pdX,pdY=pdY,sdX=sdX,sdY=sdY, roznica_x = offset2_x, roznica_y = offset2_y, offset_x=offset_x, offset2_y = 1.5*offset_y, ilosc_x = ilosc_x, ilosc_y = ilosc_y, warstwa_siatki = warstwa_siatki)
    
    
    
    ### UWAGI - offset Y to tak naprawde dlugosc boku
    
    try:
        cur.execute(query2)
        print('wszystko gra')
    except:
        print('cos poszlo nie tak')
    
    
    
    #Potwierdzenie wykonanej operacji tworzenia tabeli
    conn.commit()
    
    conn.close()


############################ II ######################################
###########  ANALIZA STATYSTYCZNA NA PODSTAWIE HEKSAGONOW ############



def statystyki_heksagony(warstwa_zasiegu = "gbh_rookeries", warstwa_docelowa = "nowa_04", pole_wagowe=0, warstwa_siatki = 'hex_grid_3'):
    """
    Funkcja pobiera od uzytkownika informacje o warstwie, na podstawie ktorej ma zostac okreslona wartosciowosc szesciokatow foremnych. Dodatkowo pozwala na wybor pola wagowego. 
    W przypadku, gdy parametr pola wagowego pozostaje pusty, wartosci heksagonow zostaja obliczone na podstawie: dla punktow - ilosci obiektow, dla linii - dlugosci linii, dla poligonow - powierzchni poligonow.
    
    """
    
    
    #### PRZYGOTOWANIE POLACZAENIA Z BAZA DANYCH
    
    try:
    
        conn = psycopg2.connect(host="localhost",database=nazwa_bazy, user=uzytkownik, password=haslo)
        print('OK')

    except:
        print('Cos poszlo nie tak...')
    
    
    
    cur = conn.cursor() # Utworzenie kursora
    
    
    query = "select distinct(st_geometrytype(geom)) from {};".format(warstwa_zasiegu) # zapytanie pobierajace typ geometrii warstwy
    cur.execute(query)
    typ_warstwy = cur.fetchone()
    typ_warstwy=str(list(typ_warstwy)) # konwersja na string - porownanie znak po znaku
    
    
    ### WARSTWA PUNKTOWA ###
    
    if typ_warstwy == "['ST_Point']" or "['ST_MultiPoint']":
        if pole_wagowe != 0:
            query2 = " "
            print("pierwszy if")
        else:
            try:
                query2 = "select a.gid, a.geom, count(b.geom) INTO {warstwa_docelowa} from {warstwa_siatki} as a, {warstwa_zasiegu} as b where st_intersects(a.geom, b.geom)=true group by a.gid; alter table {warstwa_docelowa} ADD PRIMARY KEY (gid); ".format(warstwa_docelowa = warstwa_docelowa, warstwa_zasiegu = warstwa_zasiegu, warstwa_siatki = warstwa_siatki)
                cur.execute(query2)
                conn.commit()
                conn.close()
                print("wszystko gra")
            except:
                print("Cos poszlo nie tak dla punktu...")
    else: 
        print('nie widzi ST_Point...')
        
    
    
    ### WARSTWA LINIOWA ###
    
    if typ_warstwy == "['ST_MultiLineString']":
        if pole_wagowe != 0:
            query2 = " "
            print("pierwszy if")
        else:
            try:
                query2 = "select sum(st_length(st_intersection(a.geom,b.geom))),a.geom, a.gid INTO {} from {warstwa_siatki} as a, {} as b where st_intersects(b.geom, a.geom) group by a.gid;".format(warstwa_docelowa, warstwa_zasiegu, warstwa_siatki=warstwa_siatki)
                cur.execute(query2)
                conn.commit()
                conn.close()
                print("wszystko gra")
            except:
                print("Cos poszlo nie tak dla linii...")
    else: 
        print('nie widzi ST_MultiLineString...')
   
    
    
    ### WARSTWA POLIGONOWA ###
    
    if typ_warstwy == "['ST_MultiPolygon']":
        if pole_wagowe != 0:
            query2 = " "
            print("pierwszy if")
        else:
            try:
                query2 = "select sum(st_area(st_intersection(a.geom,b.geom))),a.geom, a.gid INTO {warstwa_docelowa} from {warstwa_siatki} as a, {warstwa_zasiegu} as b where st_intersects(b.geom, a.geom)  group by a.gid;".format(warstwa_docelowa = warstwa_docelowa, warstwa_zasiegu = warstwa_zasiegu, warstwa_siatki=warstwa_siatki)
                cur.execute(query2)
                conn.commit()
                conn.close()
                print("wszystko gra")
            except:
                print("Cos poszlo nie tak dla poligonu...")
    else: 
        print('nie widzi ST_MultiPolygon...')
    
        
        
        
            
