# -*- coding: utf-8 -*-
"""
Created on Wed Jul 10 13:30:21 2019

@author: Adi
"""



### Packages

import psycopg2


### Global variables

#Database connection parameters
nazwa_bazy = 'database_name'
uzytkownik = 'user_name' 
haslo = 'password' 



################################ PART I - HEXAGONAL GRID ##################################


#Function arguments

srid = 4326 # EPSG Code - Coordinate System
offset_y  = 0.1 # Side length of the hexagon
warstwa_siatki = "hex_grid_name" # The name of the grid you want to create
warstwa_zasiegu = "coverage_layer" # The name of the layer for which area you want to draw the grid



def siatka_heksagonalna(srid, offset_y, warstwa_siatki, warstwa_zasiegu):
        
    """
    
    The function receives from the user following arguments: srid - coordinate system, offset_y - side length of the hexagon, warstwa_siatki - name of the layer storing the grid of regular hexagons, warstwa_zasiegu - the name of the layer for which area you want to draw the grid. 
    
    
    """
    ### Connecting with database
        
    try:
        
        conn = psycopg2.connect(host="localhost",database=nazwa_bazy, user=uzytkownik, password=haslo)
        print('OK')
    
    except:
        print('Something went wrong...')
        
        
    ### Creating a cursor
        
    cur = conn.cursor()
    
    
    
    ## Query - range
    
    query3 = """select min(ST_XMin(geom)) as minX , max(ST_XMax(geom)) as maxX, min(ST_YMin(geom)) as minY, max(ST_YMax(geom)) as maxY from {warstwa_zasiegu};""".format(warstwa_zasiegu = warstwa_zasiegu)
    
    
    cur.execute(query3)
    b = cur.fetchone()
    
    minX,maxX,minY,maxY= list(b)
    
    
    
    offset_x = offset_y*pow(3,1/2)/2  # offset_x - the height of an equilateral triangle
    
    offset2_x = offset_x*2  # real shift, horizontal difference between hexagons
    
    
    ldX,ldY = minX - offset_x, minY-0.5*offset_y
    lgX,lgY = minX- offset_x, minY+0.5*offset_y
    sgX, sgY = minX, minY + offset_y
    pgX,pgY =  minX + offset_x, minY+0.5*offset_y
    pdX,pdY =  minX + offset_x, minY-0.5*offset_y
    sdX, sdY = minX, minY - offset_y
    
    
    offset2_y = offset_y*3  # real shift - vertical difference between hexagons
    
    
    ## Calculation of the maximum value for the SQL function - generate_series
    
    zasiegX = abs(maxX - minX) # difference  max - min horizontally - layer extent 
    offsety_w_x = int(round(zasiegX/offset2_x + 1,0)) # the number of regular hexagons that should cover the horizontal layer / 1 in addition
    ilosc_x = offsety_w_x * offset2_x # calculation of the boundary range for hexagons - necessary for the SQL function
    
    zasiegY = abs(maxY - minY) # analogous steps for vertical coverage
    offsety_w_y = int(round(zasiegY/offset2_y + 1,0))
    ilosc_y = offsety_w_y * offset2_y 
    
    
    ## Query creating table in the database storing regular hexagons
    
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
    
    
    
    try:
        cur.execute(query2)
        print('OK')
    except:
        print('Something went wrong')
    
    
    
    #Commit for table creating operation
    conn.commit()
    
    conn.close()


############################ PART II ######################################
###########  STATISTICAL ANALYSIS BASED ON HEXAGONES ############


#Function arguments


warstwa_siatki = "hex_grid_name" # The name of the grid you created
warstwa_zasiegu = "coverage_layer" # The name of the layer you want to calculate statistics based on
warstwa_docelowa = "target_layer" # The name of layer you want to create - it will contain collectd hexagonal stats
pole_wagowe= 0 # Argument with the default value of zero, do not change it in the first version of the code



def statystyki_heksagony(warstwa_zasiegu, warstwa_docelowa, pole_wagowe, warstwa_siatki):
    """
    
    The function calculates statistics based on the drawn hexagons.
    
    
    """
    
    
    #### Connecting with database
    
    try:
    
        conn = psycopg2.connect(host="localhost",database=nazwa_bazy, user=uzytkownik, password=haslo)
        print('OK')

    except:
        print('Something went wrong...')
    
    
    
    cur = conn.cursor() # Creating cursor
    
    
    query = "select distinct(st_geometrytype(geom)) from {};".format(warstwa_zasiegu) # query retrieving the layer geometry type
    cur.execute(query)
    typ_warstwy = cur.fetchone()
    typ_warstwy=str(list(typ_warstwy)) # string conversion - character-by-character comparison
    
    
    ### POINT LAYER ###
    
    if typ_warstwy == "['ST_Point']" or "['ST_MultiPoint']":
        if pole_wagowe != 0:
            query2 = " "
            print("first if")
        else:
            try:
                query2 = "select a.gid, a.geom, count(b.geom) INTO {warstwa_docelowa} from {warstwa_siatki} as a, {warstwa_zasiegu} as b where st_intersects(a.geom, b.geom)=true group by a.gid; alter table {warstwa_docelowa} ADD PRIMARY KEY (gid); ".format(warstwa_docelowa = warstwa_docelowa, warstwa_zasiegu = warstwa_zasiegu, warstwa_siatki = warstwa_siatki)
                cur.execute(query2)
                conn.commit()
                conn.close()
                print("All rigtht")
            except:
                print("Something went wrong for point...")
    else: 
        print('does not see ST_Point...')
        
    
    
    ### LINE LAYER ###
    
    if typ_warstwy == "['ST_MultiLineString']":
        if pole_wagowe != 0:
            query2 = " "
            print("first if")
        else:
            try:
                query2 = "select sum(st_length(st_intersection(a.geom,b.geom))),a.geom, a.gid INTO {} from {warstwa_siatki} as a, {} as b where st_intersects(b.geom, a.geom) group by a.gid;".format(warstwa_docelowa, warstwa_zasiegu, warstwa_siatki=warstwa_siatki)
                cur.execute(query2)
                conn.commit()
                conn.close()
                print("All right")
            except:
                print("Something went wrong for line...")
    else: 
        print('does not see ST_MultiLineString...')
   
    
    
    ### POLYGON LAYER ###
    
    if typ_warstwy == "['ST_MultiPolygon']":
        if pole_wagowe != 0:
            query2 = " "
            print("first if")
        else:
            try:
                query2 = "select sum(st_area(st_intersection(a.geom,b.geom))),a.geom, a.gid INTO {warstwa_docelowa} from {warstwa_siatki} as a, {warstwa_zasiegu} as b where st_intersects(b.geom, a.geom)  group by a.gid;".format(warstwa_docelowa = warstwa_docelowa, warstwa_zasiegu = warstwa_zasiegu, warstwa_siatki=warstwa_siatki)
                cur.execute(query2)
                conn.commit()
                conn.close()
                print("All right")
            except:
                print("Something went wrong for polygon...")
    else: 
        print('does not see ST_MultiPolygon...')
    
        
        
        
            
