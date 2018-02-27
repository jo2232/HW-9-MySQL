

```python
from sqlalchemy import create_engine
import pandas as pd
from warnings import filterwarnings
import pymysql
filterwarnings('ignore', category=pymysql.Warning)
import os
```


```python
engine = create_engine('mysql+pymysql://root:n7tg541aw@localhost/sakila') 
```


```python
sql_query = """
    SELECT first_name, last_name
    FROM actor
"""

actors = pd.read_sql_query(sql_query, engine)
actors
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>first_name</th>
      <th>last_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>PENELOPE</td>
      <td>GUINESS</td>
    </tr>
    <tr>
      <th>1</th>
      <td>NICK</td>
      <td>WAHLBERG</td>
    </tr>
    <tr>
      <th>2</th>
      <td>ED</td>
      <td>CHASE</td>
    </tr>
    <tr>
      <th>3</th>
      <td>JENNIFER</td>
      <td>DAVIS</td>
    </tr>
    <tr>
      <th>4</th>
      <td>JOHNNY</td>
      <td>LOLLOBRIGIDA</td>
    </tr>
    <tr>
      <th>5</th>
      <td>BETTE</td>
      <td>NICHOLSON</td>
    </tr>
    <tr>
      <th>6</th>
      <td>GRACE</td>
      <td>MOSTEL</td>
    </tr>
    <tr>
      <th>7</th>
      <td>MATTHEW</td>
      <td>JOHANSSON</td>
    </tr>
    <tr>
      <th>8</th>
      <td>JOE</td>
      <td>SWANK</td>
    </tr>
    <tr>
      <th>9</th>
      <td>CHRISTIAN</td>
      <td>GABLE</td>
    </tr>
    <tr>
      <th>10</th>
      <td>ZERO</td>
      <td>CAGE</td>
    </tr>
    <tr>
      <th>11</th>
      <td>KARL</td>
      <td>BERRY</td>
    </tr>
    <tr>
      <th>12</th>
      <td>UMA</td>
      <td>WOOD</td>
    </tr>
    <tr>
      <th>13</th>
      <td>VIVIEN</td>
      <td>BERGEN</td>
    </tr>
    <tr>
      <th>14</th>
      <td>CUBA</td>
      <td>OLIVIER</td>
    </tr>
    <tr>
      <th>15</th>
      <td>FRED</td>
      <td>COSTNER</td>
    </tr>
    <tr>
      <th>16</th>
      <td>HELEN</td>
      <td>VOIGHT</td>
    </tr>
    <tr>
      <th>17</th>
      <td>DAN</td>
      <td>TORN</td>
    </tr>
    <tr>
      <th>18</th>
      <td>BOB</td>
      <td>FAWCETT</td>
    </tr>
    <tr>
      <th>19</th>
      <td>LUCILLE</td>
      <td>TRACY</td>
    </tr>
    <tr>
      <th>20</th>
      <td>KIRSTEN</td>
      <td>PALTROW</td>
    </tr>
    <tr>
      <th>21</th>
      <td>ELVIS</td>
      <td>MARX</td>
    </tr>
    <tr>
      <th>22</th>
      <td>SANDRA</td>
      <td>KILMER</td>
    </tr>
    <tr>
      <th>23</th>
      <td>CAMERON</td>
      <td>STREEP</td>
    </tr>
    <tr>
      <th>24</th>
      <td>KEVIN</td>
      <td>BLOOM</td>
    </tr>
    <tr>
      <th>25</th>
      <td>RIP</td>
      <td>CRAWFORD</td>
    </tr>
    <tr>
      <th>26</th>
      <td>JULIA</td>
      <td>MCQUEEN</td>
    </tr>
    <tr>
      <th>27</th>
      <td>WOODY</td>
      <td>HOFFMAN</td>
    </tr>
    <tr>
      <th>28</th>
      <td>ALEC</td>
      <td>WAYNE</td>
    </tr>
    <tr>
      <th>29</th>
      <td>SANDRA</td>
      <td>PECK</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>170</th>
      <td>OLYMPIA</td>
      <td>PFEIFFER</td>
    </tr>
    <tr>
      <th>171</th>
      <td>HARPO</td>
      <td>WILLIAMS</td>
    </tr>
    <tr>
      <th>172</th>
      <td>ALAN</td>
      <td>DREYFUSS</td>
    </tr>
    <tr>
      <th>173</th>
      <td>MICHAEL</td>
      <td>BENING</td>
    </tr>
    <tr>
      <th>174</th>
      <td>WILLIAM</td>
      <td>HACKMAN</td>
    </tr>
    <tr>
      <th>175</th>
      <td>JON</td>
      <td>CHASE</td>
    </tr>
    <tr>
      <th>176</th>
      <td>GENE</td>
      <td>MCKELLEN</td>
    </tr>
    <tr>
      <th>177</th>
      <td>LISA</td>
      <td>MONROE</td>
    </tr>
    <tr>
      <th>178</th>
      <td>ED</td>
      <td>GUINESS</td>
    </tr>
    <tr>
      <th>179</th>
      <td>JEFF</td>
      <td>SILVERSTONE</td>
    </tr>
    <tr>
      <th>180</th>
      <td>MATTHEW</td>
      <td>CARREY</td>
    </tr>
    <tr>
      <th>181</th>
      <td>DEBBIE</td>
      <td>AKROYD</td>
    </tr>
    <tr>
      <th>182</th>
      <td>RUSSELL</td>
      <td>CLOSE</td>
    </tr>
    <tr>
      <th>183</th>
      <td>HUMPHREY</td>
      <td>GARLAND</td>
    </tr>
    <tr>
      <th>184</th>
      <td>MICHAEL</td>
      <td>BOLGER</td>
    </tr>
    <tr>
      <th>185</th>
      <td>JULIA</td>
      <td>ZELLWEGER</td>
    </tr>
    <tr>
      <th>186</th>
      <td>RENEE</td>
      <td>BALL</td>
    </tr>
    <tr>
      <th>187</th>
      <td>ROCK</td>
      <td>DUKAKIS</td>
    </tr>
    <tr>
      <th>188</th>
      <td>CUBA</td>
      <td>BIRCH</td>
    </tr>
    <tr>
      <th>189</th>
      <td>AUDREY</td>
      <td>BAILEY</td>
    </tr>
    <tr>
      <th>190</th>
      <td>GREGORY</td>
      <td>GOODING</td>
    </tr>
    <tr>
      <th>191</th>
      <td>JOHN</td>
      <td>SUVARI</td>
    </tr>
    <tr>
      <th>192</th>
      <td>BURT</td>
      <td>TEMPLE</td>
    </tr>
    <tr>
      <th>193</th>
      <td>MERYL</td>
      <td>ALLEN</td>
    </tr>
    <tr>
      <th>194</th>
      <td>JAYNE</td>
      <td>SILVERSTONE</td>
    </tr>
    <tr>
      <th>195</th>
      <td>BELA</td>
      <td>WALKEN</td>
    </tr>
    <tr>
      <th>196</th>
      <td>REESE</td>
      <td>WEST</td>
    </tr>
    <tr>
      <th>197</th>
      <td>MARY</td>
      <td>KEITEL</td>
    </tr>
    <tr>
      <th>198</th>
      <td>JULIA</td>
      <td>FAWCETT</td>
    </tr>
    <tr>
      <th>199</th>
      <td>THORA</td>
      <td>TEMPLE</td>
    </tr>
  </tbody>
</table>
<p>200 rows × 2 columns</p>
</div>




```python
sql_query = """
    SELECT CONCAT(first_name, ' ', last_name) as Actor
    FROM actor
"""

concat_actors = pd.read_sql_query(sql_query, engine)
concat_actors
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Actor</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>PENELOPE GUINESS</td>
    </tr>
    <tr>
      <th>1</th>
      <td>NICK WAHLBERG</td>
    </tr>
    <tr>
      <th>2</th>
      <td>ED CHASE</td>
    </tr>
    <tr>
      <th>3</th>
      <td>JENNIFER DAVIS</td>
    </tr>
    <tr>
      <th>4</th>
      <td>JOHNNY LOLLOBRIGIDA</td>
    </tr>
    <tr>
      <th>5</th>
      <td>BETTE NICHOLSON</td>
    </tr>
    <tr>
      <th>6</th>
      <td>GRACE MOSTEL</td>
    </tr>
    <tr>
      <th>7</th>
      <td>MATTHEW JOHANSSON</td>
    </tr>
    <tr>
      <th>8</th>
      <td>JOE SWANK</td>
    </tr>
    <tr>
      <th>9</th>
      <td>CHRISTIAN GABLE</td>
    </tr>
    <tr>
      <th>10</th>
      <td>ZERO CAGE</td>
    </tr>
    <tr>
      <th>11</th>
      <td>KARL BERRY</td>
    </tr>
    <tr>
      <th>12</th>
      <td>UMA WOOD</td>
    </tr>
    <tr>
      <th>13</th>
      <td>VIVIEN BERGEN</td>
    </tr>
    <tr>
      <th>14</th>
      <td>CUBA OLIVIER</td>
    </tr>
    <tr>
      <th>15</th>
      <td>FRED COSTNER</td>
    </tr>
    <tr>
      <th>16</th>
      <td>HELEN VOIGHT</td>
    </tr>
    <tr>
      <th>17</th>
      <td>DAN TORN</td>
    </tr>
    <tr>
      <th>18</th>
      <td>BOB FAWCETT</td>
    </tr>
    <tr>
      <th>19</th>
      <td>LUCILLE TRACY</td>
    </tr>
    <tr>
      <th>20</th>
      <td>KIRSTEN PALTROW</td>
    </tr>
    <tr>
      <th>21</th>
      <td>ELVIS MARX</td>
    </tr>
    <tr>
      <th>22</th>
      <td>SANDRA KILMER</td>
    </tr>
    <tr>
      <th>23</th>
      <td>CAMERON STREEP</td>
    </tr>
    <tr>
      <th>24</th>
      <td>KEVIN BLOOM</td>
    </tr>
    <tr>
      <th>25</th>
      <td>RIP CRAWFORD</td>
    </tr>
    <tr>
      <th>26</th>
      <td>JULIA MCQUEEN</td>
    </tr>
    <tr>
      <th>27</th>
      <td>WOODY HOFFMAN</td>
    </tr>
    <tr>
      <th>28</th>
      <td>ALEC WAYNE</td>
    </tr>
    <tr>
      <th>29</th>
      <td>SANDRA PECK</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
    </tr>
    <tr>
      <th>170</th>
      <td>OLYMPIA PFEIFFER</td>
    </tr>
    <tr>
      <th>171</th>
      <td>HARPO WILLIAMS</td>
    </tr>
    <tr>
      <th>172</th>
      <td>ALAN DREYFUSS</td>
    </tr>
    <tr>
      <th>173</th>
      <td>MICHAEL BENING</td>
    </tr>
    <tr>
      <th>174</th>
      <td>WILLIAM HACKMAN</td>
    </tr>
    <tr>
      <th>175</th>
      <td>JON CHASE</td>
    </tr>
    <tr>
      <th>176</th>
      <td>GENE MCKELLEN</td>
    </tr>
    <tr>
      <th>177</th>
      <td>LISA MONROE</td>
    </tr>
    <tr>
      <th>178</th>
      <td>ED GUINESS</td>
    </tr>
    <tr>
      <th>179</th>
      <td>JEFF SILVERSTONE</td>
    </tr>
    <tr>
      <th>180</th>
      <td>MATTHEW CARREY</td>
    </tr>
    <tr>
      <th>181</th>
      <td>DEBBIE AKROYD</td>
    </tr>
    <tr>
      <th>182</th>
      <td>RUSSELL CLOSE</td>
    </tr>
    <tr>
      <th>183</th>
      <td>HUMPHREY GARLAND</td>
    </tr>
    <tr>
      <th>184</th>
      <td>MICHAEL BOLGER</td>
    </tr>
    <tr>
      <th>185</th>
      <td>JULIA ZELLWEGER</td>
    </tr>
    <tr>
      <th>186</th>
      <td>RENEE BALL</td>
    </tr>
    <tr>
      <th>187</th>
      <td>ROCK DUKAKIS</td>
    </tr>
    <tr>
      <th>188</th>
      <td>CUBA BIRCH</td>
    </tr>
    <tr>
      <th>189</th>
      <td>AUDREY BAILEY</td>
    </tr>
    <tr>
      <th>190</th>
      <td>GREGORY GOODING</td>
    </tr>
    <tr>
      <th>191</th>
      <td>JOHN SUVARI</td>
    </tr>
    <tr>
      <th>192</th>
      <td>BURT TEMPLE</td>
    </tr>
    <tr>
      <th>193</th>
      <td>MERYL ALLEN</td>
    </tr>
    <tr>
      <th>194</th>
      <td>JAYNE SILVERSTONE</td>
    </tr>
    <tr>
      <th>195</th>
      <td>BELA WALKEN</td>
    </tr>
    <tr>
      <th>196</th>
      <td>REESE WEST</td>
    </tr>
    <tr>
      <th>197</th>
      <td>MARY KEITEL</td>
    </tr>
    <tr>
      <th>198</th>
      <td>JULIA FAWCETT</td>
    </tr>
    <tr>
      <th>199</th>
      <td>THORA TEMPLE</td>
    </tr>
  </tbody>
</table>
<p>200 rows × 1 columns</p>
</div>




```python
sql_query = """
    SELECT actor_id, first_name, last_name 
    FROM actor
    Where first_name Like '%%Joe%%';
"""

joe_actor = pd.read_sql_query(sql_query, engine)
joe_actor
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>actor_id</th>
      <th>first_name</th>
      <th>last_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>9</td>
      <td>JOE</td>
      <td>SWANK</td>
    </tr>
  </tbody>
</table>
</div>




```python
sql_query = """
    SELECT actor_id, first_name, last_name 
    FROM actor
    Where last_name Like '%%Gen%%';
"""

gen_actors = pd.read_sql_query(sql_query, engine)
gen_actors
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>actor_id</th>
      <th>first_name</th>
      <th>last_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>14</td>
      <td>VIVIEN</td>
      <td>BERGEN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>41</td>
      <td>JODIE</td>
      <td>DEGENERES</td>
    </tr>
    <tr>
      <th>2</th>
      <td>107</td>
      <td>GINA</td>
      <td>DEGENERES</td>
    </tr>
    <tr>
      <th>3</th>
      <td>166</td>
      <td>NICK</td>
      <td>DEGENERES</td>
    </tr>
  </tbody>
</table>
</div>




```python
sql_query = """
    SELECT first_name, last_name 
    FROM actor
    WHERE last_name Like '%%LI%%'
    ORDER by last_name, first_name;
"""

gen_actors = pd.read_sql_query(sql_query, engine)
gen_actors
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>first_name</th>
      <th>last_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>GREG</td>
      <td>CHAPLIN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>WOODY</td>
      <td>JOLIE</td>
    </tr>
    <tr>
      <th>2</th>
      <td>AUDREY</td>
      <td>OLIVIER</td>
    </tr>
    <tr>
      <th>3</th>
      <td>CUBA</td>
      <td>OLIVIER</td>
    </tr>
    <tr>
      <th>4</th>
      <td>HARPO</td>
      <td>WILLIAMS</td>
    </tr>
    <tr>
      <th>5</th>
      <td>MORGAN</td>
      <td>WILLIAMS</td>
    </tr>
    <tr>
      <th>6</th>
      <td>SEAN</td>
      <td>WILLIAMS</td>
    </tr>
    <tr>
      <th>7</th>
      <td>BEN</td>
      <td>WILLIS</td>
    </tr>
    <tr>
      <th>8</th>
      <td>GENE</td>
      <td>WILLIS</td>
    </tr>
    <tr>
      <th>9</th>
      <td>HUMPHREY</td>
      <td>WILLIS</td>
    </tr>
  </tbody>
</table>
</div>




```python
#2.d
sql_query = """
select country_id, country
from country
where country in ('Afghanistan', 'Bangladesh', 'China');
"""

country = pd.read_sql_query(sql_query, engine)
country
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>country_id</th>
      <th>country</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Afghanistan</td>
    </tr>
    <tr>
      <th>1</th>
      <td>12</td>
      <td>Bangladesh</td>
    </tr>
    <tr>
      <th>2</th>
      <td>23</td>
      <td>China</td>
    </tr>
  </tbody>
</table>
</div>




```python
#3.a
sql_query = """
ALTER TABLE actor 
ADD middle_name varchar(255) AFTER first_name;
"""

```


```python
#3.b
sql_query = """
ALTER TABLE actor 
ALTER COLUMN middle_name blobs;
"""
```


```python
#3.c
sql_query = """
ALTER TABLE actor 
DROP COLUMN middle_name;
"""
```


```python
#4.a
sql_query = """
SELECT last_name, COUNT(*)
FROM actor
GROUP BY last_name;
"""
actor_last = pd.read_sql_query(sql_query, engine)
actor_last
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>last_name</th>
      <th>COUNT(*)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>AKROYD</td>
      <td>3</td>
    </tr>
    <tr>
      <th>1</th>
      <td>ALLEN</td>
      <td>3</td>
    </tr>
    <tr>
      <th>2</th>
      <td>ASTAIRE</td>
      <td>1</td>
    </tr>
    <tr>
      <th>3</th>
      <td>BACALL</td>
      <td>1</td>
    </tr>
    <tr>
      <th>4</th>
      <td>BAILEY</td>
      <td>2</td>
    </tr>
    <tr>
      <th>5</th>
      <td>BALE</td>
      <td>1</td>
    </tr>
    <tr>
      <th>6</th>
      <td>BALL</td>
      <td>1</td>
    </tr>
    <tr>
      <th>7</th>
      <td>BARRYMORE</td>
      <td>1</td>
    </tr>
    <tr>
      <th>8</th>
      <td>BASINGER</td>
      <td>1</td>
    </tr>
    <tr>
      <th>9</th>
      <td>BENING</td>
      <td>2</td>
    </tr>
    <tr>
      <th>10</th>
      <td>BERGEN</td>
      <td>1</td>
    </tr>
    <tr>
      <th>11</th>
      <td>BERGMAN</td>
      <td>1</td>
    </tr>
    <tr>
      <th>12</th>
      <td>BERRY</td>
      <td>3</td>
    </tr>
    <tr>
      <th>13</th>
      <td>BIRCH</td>
      <td>1</td>
    </tr>
    <tr>
      <th>14</th>
      <td>BLOOM</td>
      <td>1</td>
    </tr>
    <tr>
      <th>15</th>
      <td>BOLGER</td>
      <td>2</td>
    </tr>
    <tr>
      <th>16</th>
      <td>BRIDGES</td>
      <td>1</td>
    </tr>
    <tr>
      <th>17</th>
      <td>BRODY</td>
      <td>2</td>
    </tr>
    <tr>
      <th>18</th>
      <td>BULLOCK</td>
      <td>1</td>
    </tr>
    <tr>
      <th>19</th>
      <td>CAGE</td>
      <td>2</td>
    </tr>
    <tr>
      <th>20</th>
      <td>CARREY</td>
      <td>1</td>
    </tr>
    <tr>
      <th>21</th>
      <td>CHAPLIN</td>
      <td>1</td>
    </tr>
    <tr>
      <th>22</th>
      <td>CHASE</td>
      <td>2</td>
    </tr>
    <tr>
      <th>23</th>
      <td>CLOSE</td>
      <td>1</td>
    </tr>
    <tr>
      <th>24</th>
      <td>COSTNER</td>
      <td>1</td>
    </tr>
    <tr>
      <th>25</th>
      <td>CRAWFORD</td>
      <td>2</td>
    </tr>
    <tr>
      <th>26</th>
      <td>CRONYN</td>
      <td>2</td>
    </tr>
    <tr>
      <th>27</th>
      <td>CROWE</td>
      <td>1</td>
    </tr>
    <tr>
      <th>28</th>
      <td>CRUISE</td>
      <td>1</td>
    </tr>
    <tr>
      <th>29</th>
      <td>CRUZ</td>
      <td>1</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>91</th>
      <td>POSEY</td>
      <td>1</td>
    </tr>
    <tr>
      <th>92</th>
      <td>PRESLEY</td>
      <td>1</td>
    </tr>
    <tr>
      <th>93</th>
      <td>REYNOLDS</td>
      <td>1</td>
    </tr>
    <tr>
      <th>94</th>
      <td>RYDER</td>
      <td>1</td>
    </tr>
    <tr>
      <th>95</th>
      <td>SILVERSTONE</td>
      <td>2</td>
    </tr>
    <tr>
      <th>96</th>
      <td>SINATRA</td>
      <td>1</td>
    </tr>
    <tr>
      <th>97</th>
      <td>SOBIESKI</td>
      <td>1</td>
    </tr>
    <tr>
      <th>98</th>
      <td>STALLONE</td>
      <td>1</td>
    </tr>
    <tr>
      <th>99</th>
      <td>STREEP</td>
      <td>2</td>
    </tr>
    <tr>
      <th>100</th>
      <td>SUVARI</td>
      <td>1</td>
    </tr>
    <tr>
      <th>101</th>
      <td>SWANK</td>
      <td>1</td>
    </tr>
    <tr>
      <th>102</th>
      <td>TANDY</td>
      <td>2</td>
    </tr>
    <tr>
      <th>103</th>
      <td>TAUTOU</td>
      <td>1</td>
    </tr>
    <tr>
      <th>104</th>
      <td>TEMPLE</td>
      <td>4</td>
    </tr>
    <tr>
      <th>105</th>
      <td>TOMEI</td>
      <td>1</td>
    </tr>
    <tr>
      <th>106</th>
      <td>TORN</td>
      <td>3</td>
    </tr>
    <tr>
      <th>107</th>
      <td>TRACY</td>
      <td>2</td>
    </tr>
    <tr>
      <th>108</th>
      <td>VOIGHT</td>
      <td>1</td>
    </tr>
    <tr>
      <th>109</th>
      <td>WAHLBERG</td>
      <td>2</td>
    </tr>
    <tr>
      <th>110</th>
      <td>WALKEN</td>
      <td>1</td>
    </tr>
    <tr>
      <th>111</th>
      <td>WAYNE</td>
      <td>1</td>
    </tr>
    <tr>
      <th>112</th>
      <td>WEST</td>
      <td>2</td>
    </tr>
    <tr>
      <th>113</th>
      <td>WILLIAMS</td>
      <td>3</td>
    </tr>
    <tr>
      <th>114</th>
      <td>WILLIS</td>
      <td>3</td>
    </tr>
    <tr>
      <th>115</th>
      <td>WILSON</td>
      <td>1</td>
    </tr>
    <tr>
      <th>116</th>
      <td>WINSLET</td>
      <td>2</td>
    </tr>
    <tr>
      <th>117</th>
      <td>WITHERSPOON</td>
      <td>1</td>
    </tr>
    <tr>
      <th>118</th>
      <td>WOOD</td>
      <td>2</td>
    </tr>
    <tr>
      <th>119</th>
      <td>WRAY</td>
      <td>1</td>
    </tr>
    <tr>
      <th>120</th>
      <td>ZELLWEGER</td>
      <td>3</td>
    </tr>
  </tbody>
</table>
<p>121 rows × 2 columns</p>
</div>




```python
#4.b
sql_query = """
SELECT last_name, COUNT(*) as Numbers
FROM actor
GROUP BY last_name
HAVING Numbers > 1;
"""
last_names = pd.read_sql_query(sql_query, engine)
last_names
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>last_name</th>
      <th>Numbers</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>AKROYD</td>
      <td>3</td>
    </tr>
    <tr>
      <th>1</th>
      <td>ALLEN</td>
      <td>3</td>
    </tr>
    <tr>
      <th>2</th>
      <td>BAILEY</td>
      <td>2</td>
    </tr>
    <tr>
      <th>3</th>
      <td>BENING</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4</th>
      <td>BERRY</td>
      <td>3</td>
    </tr>
    <tr>
      <th>5</th>
      <td>BOLGER</td>
      <td>2</td>
    </tr>
    <tr>
      <th>6</th>
      <td>BRODY</td>
      <td>2</td>
    </tr>
    <tr>
      <th>7</th>
      <td>CAGE</td>
      <td>2</td>
    </tr>
    <tr>
      <th>8</th>
      <td>CHASE</td>
      <td>2</td>
    </tr>
    <tr>
      <th>9</th>
      <td>CRAWFORD</td>
      <td>2</td>
    </tr>
    <tr>
      <th>10</th>
      <td>CRONYN</td>
      <td>2</td>
    </tr>
    <tr>
      <th>11</th>
      <td>DAVIS</td>
      <td>3</td>
    </tr>
    <tr>
      <th>12</th>
      <td>DEAN</td>
      <td>2</td>
    </tr>
    <tr>
      <th>13</th>
      <td>DEE</td>
      <td>2</td>
    </tr>
    <tr>
      <th>14</th>
      <td>DEGENERES</td>
      <td>3</td>
    </tr>
    <tr>
      <th>15</th>
      <td>DENCH</td>
      <td>2</td>
    </tr>
    <tr>
      <th>16</th>
      <td>DEPP</td>
      <td>2</td>
    </tr>
    <tr>
      <th>17</th>
      <td>DUKAKIS</td>
      <td>2</td>
    </tr>
    <tr>
      <th>18</th>
      <td>FAWCETT</td>
      <td>2</td>
    </tr>
    <tr>
      <th>19</th>
      <td>GARLAND</td>
      <td>3</td>
    </tr>
    <tr>
      <th>20</th>
      <td>GOODING</td>
      <td>2</td>
    </tr>
    <tr>
      <th>21</th>
      <td>GUINESS</td>
      <td>3</td>
    </tr>
    <tr>
      <th>22</th>
      <td>HACKMAN</td>
      <td>2</td>
    </tr>
    <tr>
      <th>23</th>
      <td>HARRIS</td>
      <td>3</td>
    </tr>
    <tr>
      <th>24</th>
      <td>HOFFMAN</td>
      <td>3</td>
    </tr>
    <tr>
      <th>25</th>
      <td>HOPKINS</td>
      <td>3</td>
    </tr>
    <tr>
      <th>26</th>
      <td>HOPPER</td>
      <td>2</td>
    </tr>
    <tr>
      <th>27</th>
      <td>JACKMAN</td>
      <td>2</td>
    </tr>
    <tr>
      <th>28</th>
      <td>JOHANSSON</td>
      <td>3</td>
    </tr>
    <tr>
      <th>29</th>
      <td>KEITEL</td>
      <td>3</td>
    </tr>
    <tr>
      <th>30</th>
      <td>KILMER</td>
      <td>5</td>
    </tr>
    <tr>
      <th>31</th>
      <td>MCCONAUGHEY</td>
      <td>2</td>
    </tr>
    <tr>
      <th>32</th>
      <td>MCKELLEN</td>
      <td>2</td>
    </tr>
    <tr>
      <th>33</th>
      <td>MCQUEEN</td>
      <td>2</td>
    </tr>
    <tr>
      <th>34</th>
      <td>MONROE</td>
      <td>2</td>
    </tr>
    <tr>
      <th>35</th>
      <td>MOSTEL</td>
      <td>2</td>
    </tr>
    <tr>
      <th>36</th>
      <td>NEESON</td>
      <td>2</td>
    </tr>
    <tr>
      <th>37</th>
      <td>NOLTE</td>
      <td>4</td>
    </tr>
    <tr>
      <th>38</th>
      <td>OLIVIER</td>
      <td>2</td>
    </tr>
    <tr>
      <th>39</th>
      <td>PALTROW</td>
      <td>2</td>
    </tr>
    <tr>
      <th>40</th>
      <td>PECK</td>
      <td>3</td>
    </tr>
    <tr>
      <th>41</th>
      <td>PENN</td>
      <td>2</td>
    </tr>
    <tr>
      <th>42</th>
      <td>SILVERSTONE</td>
      <td>2</td>
    </tr>
    <tr>
      <th>43</th>
      <td>STREEP</td>
      <td>2</td>
    </tr>
    <tr>
      <th>44</th>
      <td>TANDY</td>
      <td>2</td>
    </tr>
    <tr>
      <th>45</th>
      <td>TEMPLE</td>
      <td>4</td>
    </tr>
    <tr>
      <th>46</th>
      <td>TORN</td>
      <td>3</td>
    </tr>
    <tr>
      <th>47</th>
      <td>TRACY</td>
      <td>2</td>
    </tr>
    <tr>
      <th>48</th>
      <td>WAHLBERG</td>
      <td>2</td>
    </tr>
    <tr>
      <th>49</th>
      <td>WEST</td>
      <td>2</td>
    </tr>
    <tr>
      <th>50</th>
      <td>WILLIAMS</td>
      <td>3</td>
    </tr>
    <tr>
      <th>51</th>
      <td>WILLIS</td>
      <td>3</td>
    </tr>
    <tr>
      <th>52</th>
      <td>WINSLET</td>
      <td>2</td>
    </tr>
    <tr>
      <th>53</th>
      <td>WOOD</td>
      <td>2</td>
    </tr>
    <tr>
      <th>54</th>
      <td>ZELLWEGER</td>
      <td>3</td>
    </tr>
  </tbody>
</table>
</div>




```python
#4.c
sql_query = """
UPDATE actor
SET first_name ='HARPO'
WHERE actor_id ='172';
"""
```


```python
#4.d
sql_query = """
UPDATE actor
SET first_name ='Groucho'
WHERE actor_id ='172';
"""
```


```python
#5.a
sql_query = """
SHOW CREATE TABLE address;
"""
address_table = pd.read_sql_query(sql_query, engine)
address_table
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Table</th>
      <th>Create Table</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>address</td>
      <td>CREATE TABLE `address` (\n  `address_id` small...</td>
    </tr>
  </tbody>
</table>
</div>




```python
#6.a
sql_query = """
select first_name, last_name, address from staff 
inner join address on 
address.address_id = staff.address_id
"""
joined = pd.read_sql_query(sql_query, engine)
joined.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>first_name</th>
      <th>last_name</th>
      <th>address</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Mike</td>
      <td>Hillyer</td>
      <td>23 Workhaven Lane</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Jon</td>
      <td>Stephens</td>
      <td>1411 Lillydale Drive</td>
    </tr>
  </tbody>
</table>
</div>




```python
#6.b
sql_query = """
select first_name, last_name, staff.staff_id, SUM(amount) from staff 
inner join payment on payment.staff_id = staff.staff_id
GROUP BY staff_id;
"""
pay_amount = pd.read_sql_query(sql_query, engine)
pay_amount.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>first_name</th>
      <th>last_name</th>
      <th>staff_id</th>
      <th>SUM(amount)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Mike</td>
      <td>Hillyer</td>
      <td>1</td>
      <td>33489.47</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Jon</td>
      <td>Stephens</td>
      <td>2</td>
      <td>33927.04</td>
    </tr>
  </tbody>
</table>
</div>




```python
#6.c
sql_query = """
select title, COUNT(film_actor.actor_id) from film 
inner join film_actor on film_actor.film_id = film.film_id
GROUP BY film.title;
"""
film_actor_count = pd.read_sql_query(sql_query, engine)
film_actor_count
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>title</th>
      <th>COUNT(film_actor.actor_id)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>ACADEMY DINOSAUR</td>
      <td>10</td>
    </tr>
    <tr>
      <th>1</th>
      <td>ACE GOLDFINGER</td>
      <td>4</td>
    </tr>
    <tr>
      <th>2</th>
      <td>ADAPTATION HOLES</td>
      <td>5</td>
    </tr>
    <tr>
      <th>3</th>
      <td>AFFAIR PREJUDICE</td>
      <td>5</td>
    </tr>
    <tr>
      <th>4</th>
      <td>AFRICAN EGG</td>
      <td>5</td>
    </tr>
    <tr>
      <th>5</th>
      <td>AGENT TRUMAN</td>
      <td>7</td>
    </tr>
    <tr>
      <th>6</th>
      <td>AIRPLANE SIERRA</td>
      <td>5</td>
    </tr>
    <tr>
      <th>7</th>
      <td>AIRPORT POLLOCK</td>
      <td>4</td>
    </tr>
    <tr>
      <th>8</th>
      <td>ALABAMA DEVIL</td>
      <td>9</td>
    </tr>
    <tr>
      <th>9</th>
      <td>ALADDIN CALENDAR</td>
      <td>8</td>
    </tr>
    <tr>
      <th>10</th>
      <td>ALAMO VIDEOTAPE</td>
      <td>4</td>
    </tr>
    <tr>
      <th>11</th>
      <td>ALASKA PHANTOM</td>
      <td>7</td>
    </tr>
    <tr>
      <th>12</th>
      <td>ALI FOREVER</td>
      <td>5</td>
    </tr>
    <tr>
      <th>13</th>
      <td>ALICE FANTASIA</td>
      <td>4</td>
    </tr>
    <tr>
      <th>14</th>
      <td>ALIEN CENTER</td>
      <td>6</td>
    </tr>
    <tr>
      <th>15</th>
      <td>ALLEY EVOLUTION</td>
      <td>5</td>
    </tr>
    <tr>
      <th>16</th>
      <td>ALONE TRIP</td>
      <td>8</td>
    </tr>
    <tr>
      <th>17</th>
      <td>ALTER VICTORY</td>
      <td>4</td>
    </tr>
    <tr>
      <th>18</th>
      <td>AMADEUS HOLY</td>
      <td>6</td>
    </tr>
    <tr>
      <th>19</th>
      <td>AMELIE HELLFIGHTERS</td>
      <td>6</td>
    </tr>
    <tr>
      <th>20</th>
      <td>AMERICAN CIRCUS</td>
      <td>5</td>
    </tr>
    <tr>
      <th>21</th>
      <td>AMISTAD MIDSUMMER</td>
      <td>4</td>
    </tr>
    <tr>
      <th>22</th>
      <td>ANACONDA CONFESSIONS</td>
      <td>5</td>
    </tr>
    <tr>
      <th>23</th>
      <td>ANALYZE HOOSIERS</td>
      <td>5</td>
    </tr>
    <tr>
      <th>24</th>
      <td>ANGELS LIFE</td>
      <td>9</td>
    </tr>
    <tr>
      <th>25</th>
      <td>ANNIE IDENTITY</td>
      <td>3</td>
    </tr>
    <tr>
      <th>26</th>
      <td>ANONYMOUS HUMAN</td>
      <td>9</td>
    </tr>
    <tr>
      <th>27</th>
      <td>ANTHEM LUKE</td>
      <td>2</td>
    </tr>
    <tr>
      <th>28</th>
      <td>ANTITRUST TOMATOES</td>
      <td>7</td>
    </tr>
    <tr>
      <th>29</th>
      <td>ANYTHING SAVANNAH</td>
      <td>3</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>967</th>
      <td>WHALE BIKINI</td>
      <td>5</td>
    </tr>
    <tr>
      <th>968</th>
      <td>WHISPERER GIANT</td>
      <td>3</td>
    </tr>
    <tr>
      <th>969</th>
      <td>WIFE TURN</td>
      <td>6</td>
    </tr>
    <tr>
      <th>970</th>
      <td>WILD APOLLO</td>
      <td>4</td>
    </tr>
    <tr>
      <th>971</th>
      <td>WILLOW TRACY</td>
      <td>2</td>
    </tr>
    <tr>
      <th>972</th>
      <td>WIND PHANTOM</td>
      <td>3</td>
    </tr>
    <tr>
      <th>973</th>
      <td>WINDOW SIDE</td>
      <td>4</td>
    </tr>
    <tr>
      <th>974</th>
      <td>WISDOM WORKER</td>
      <td>7</td>
    </tr>
    <tr>
      <th>975</th>
      <td>WITCHES PANIC</td>
      <td>4</td>
    </tr>
    <tr>
      <th>976</th>
      <td>WIZARD COLDBLOODED</td>
      <td>9</td>
    </tr>
    <tr>
      <th>977</th>
      <td>WOLVES DESIRE</td>
      <td>6</td>
    </tr>
    <tr>
      <th>978</th>
      <td>WOMEN DORADO</td>
      <td>5</td>
    </tr>
    <tr>
      <th>979</th>
      <td>WON DARES</td>
      <td>5</td>
    </tr>
    <tr>
      <th>980</th>
      <td>WONDERFUL DROP</td>
      <td>4</td>
    </tr>
    <tr>
      <th>981</th>
      <td>WONDERLAND CHRISTMAS</td>
      <td>5</td>
    </tr>
    <tr>
      <th>982</th>
      <td>WONKA SEA</td>
      <td>2</td>
    </tr>
    <tr>
      <th>983</th>
      <td>WORDS HUNTER</td>
      <td>6</td>
    </tr>
    <tr>
      <th>984</th>
      <td>WORKER TARZAN</td>
      <td>9</td>
    </tr>
    <tr>
      <th>985</th>
      <td>WORKING MICROCOSMOS</td>
      <td>5</td>
    </tr>
    <tr>
      <th>986</th>
      <td>WORLD LEATHERNECKS</td>
      <td>8</td>
    </tr>
    <tr>
      <th>987</th>
      <td>WORST BANGER</td>
      <td>4</td>
    </tr>
    <tr>
      <th>988</th>
      <td>WRATH MILE</td>
      <td>4</td>
    </tr>
    <tr>
      <th>989</th>
      <td>WRONG BEHAVIOR</td>
      <td>9</td>
    </tr>
    <tr>
      <th>990</th>
      <td>WYOMING STORM</td>
      <td>6</td>
    </tr>
    <tr>
      <th>991</th>
      <td>YENTL IDAHO</td>
      <td>1</td>
    </tr>
    <tr>
      <th>992</th>
      <td>YOUNG LANGUAGE</td>
      <td>5</td>
    </tr>
    <tr>
      <th>993</th>
      <td>YOUTH KICK</td>
      <td>5</td>
    </tr>
    <tr>
      <th>994</th>
      <td>ZHIVAGO CORE</td>
      <td>6</td>
    </tr>
    <tr>
      <th>995</th>
      <td>ZOOLANDER FICTION</td>
      <td>5</td>
    </tr>
    <tr>
      <th>996</th>
      <td>ZORRO ARK</td>
      <td>3</td>
    </tr>
  </tbody>
</table>
<p>997 rows × 2 columns</p>
</div>




```python
#6.d
sql_query = """
select COUNT(inventory_id) from inventory 
Where film_id = 439 ;
"""
hunchback = pd.read_sql_query(sql_query, engine)
hunchback
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>COUNT(inventory_id)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>6</td>
    </tr>
  </tbody>
</table>
</div>




```python
#6.e
sql_query = """
select first_name, last_name, SUM(amount) as 'Total Amount Paid' from customer 
inner join payment on payment.customer_id = customer.customer_id
GROUP BY customer.customer_id
ORDER BY last_name;
"""
customer_pay = pd.read_sql_query(sql_query, engine)
customer_pay.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>first_name</th>
      <th>last_name</th>
      <th>Total Amount Paid</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>RAFAEL</td>
      <td>ABNEY</td>
      <td>97.79</td>
    </tr>
    <tr>
      <th>1</th>
      <td>NATHANIEL</td>
      <td>ADAM</td>
      <td>133.72</td>
    </tr>
    <tr>
      <th>2</th>
      <td>KATHLEEN</td>
      <td>ADAMS</td>
      <td>92.73</td>
    </tr>
    <tr>
      <th>3</th>
      <td>DIANA</td>
      <td>ALEXANDER</td>
      <td>105.73</td>
    </tr>
    <tr>
      <th>4</th>
      <td>GORDON</td>
      <td>ALLARD</td>
      <td>160.68</td>
    </tr>
  </tbody>
</table>
</div>




```python
#7.a
sql_query = """
select title
from film
where (title like 'K%%' OR title like 'Q%%') 
AND film_id IN
(
  select film.film_id
  from film
  Where language_id = 1
);
"""
K_Q = pd.read_sql_query(sql_query, engine)
K_Q
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>title</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>KANE EXORCIST</td>
    </tr>
    <tr>
      <th>1</th>
      <td>KARATE MOON</td>
    </tr>
    <tr>
      <th>2</th>
      <td>KENTUCKIAN GIANT</td>
    </tr>
    <tr>
      <th>3</th>
      <td>KICK SAVANNAH</td>
    </tr>
    <tr>
      <th>4</th>
      <td>KILL BROTHERHOOD</td>
    </tr>
    <tr>
      <th>5</th>
      <td>KILLER INNOCENT</td>
    </tr>
    <tr>
      <th>6</th>
      <td>KING EVOLUTION</td>
    </tr>
    <tr>
      <th>7</th>
      <td>KISS GLORY</td>
    </tr>
    <tr>
      <th>8</th>
      <td>KISSING DOLLS</td>
    </tr>
    <tr>
      <th>9</th>
      <td>KNOCK WARLOCK</td>
    </tr>
    <tr>
      <th>10</th>
      <td>KRAMER CHOCOLATE</td>
    </tr>
    <tr>
      <th>11</th>
      <td>KWAI HOMEWARD</td>
    </tr>
    <tr>
      <th>12</th>
      <td>QUEEN LUKE</td>
    </tr>
    <tr>
      <th>13</th>
      <td>QUEST MUSSOLINI</td>
    </tr>
    <tr>
      <th>14</th>
      <td>QUILLS BULL</td>
    </tr>
  </tbody>
</table>
</div>




```python
#7.b
sql_query = """
select first_name, last_name
from actor
where actor_id in
(
  select film_actor.actor_id
  from film_actor
  Where film_id in
  ( 
   select film_id
   from film
   Where title = "Alone Trip" 
  )
);
"""
alone_trip = pd.read_sql_query(sql_query, engine)
alone_trip
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>first_name</th>
      <th>last_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>ED</td>
      <td>CHASE</td>
    </tr>
    <tr>
      <th>1</th>
      <td>KARL</td>
      <td>BERRY</td>
    </tr>
    <tr>
      <th>2</th>
      <td>UMA</td>
      <td>WOOD</td>
    </tr>
    <tr>
      <th>3</th>
      <td>WOODY</td>
      <td>JOLIE</td>
    </tr>
    <tr>
      <th>4</th>
      <td>SPENCER</td>
      <td>DEPP</td>
    </tr>
    <tr>
      <th>5</th>
      <td>CHRIS</td>
      <td>DEPP</td>
    </tr>
    <tr>
      <th>6</th>
      <td>LAURENCE</td>
      <td>BULLOCK</td>
    </tr>
    <tr>
      <th>7</th>
      <td>RENEE</td>
      <td>BALL</td>
    </tr>
  </tbody>
</table>
</div>




```python
#7.c
sql_query = """
select first_name, last_name, email, country 
from customer
join address
on address.address_id = customer.address_id
join city
on city.city_id = address.city_id
join country
on country.country_id = city.country_id
Where country = 'Canada';
"""
customer_country = pd.read_sql_query(sql_query, engine)
customer_country
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>first_name</th>
      <th>last_name</th>
      <th>email</th>
      <th>country</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>DERRICK</td>
      <td>BOURQUE</td>
      <td>DERRICK.BOURQUE@sakilacustomer.org</td>
      <td>Canada</td>
    </tr>
    <tr>
      <th>1</th>
      <td>DARRELL</td>
      <td>POWER</td>
      <td>DARRELL.POWER@sakilacustomer.org</td>
      <td>Canada</td>
    </tr>
    <tr>
      <th>2</th>
      <td>LORETTA</td>
      <td>CARPENTER</td>
      <td>LORETTA.CARPENTER@sakilacustomer.org</td>
      <td>Canada</td>
    </tr>
    <tr>
      <th>3</th>
      <td>CURTIS</td>
      <td>IRBY</td>
      <td>CURTIS.IRBY@sakilacustomer.org</td>
      <td>Canada</td>
    </tr>
    <tr>
      <th>4</th>
      <td>TROY</td>
      <td>QUIGLEY</td>
      <td>TROY.QUIGLEY@sakilacustomer.org</td>
      <td>Canada</td>
    </tr>
  </tbody>
</table>
</div>




```python
#7.d
sql_query = """
select title  
from film
join film_category
on film_category.film_id = film.film_id
join category
on category.category_id = film_category.category_id
Where name = 'Family';
"""
family_cat = pd.read_sql_query(sql_query, engine)
family_cat
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>title</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>AFRICAN EGG</td>
    </tr>
    <tr>
      <th>1</th>
      <td>APACHE DIVINE</td>
    </tr>
    <tr>
      <th>2</th>
      <td>ATLANTIS CAUSE</td>
    </tr>
    <tr>
      <th>3</th>
      <td>BAKED CLEOPATRA</td>
    </tr>
    <tr>
      <th>4</th>
      <td>BANG KWAI</td>
    </tr>
    <tr>
      <th>5</th>
      <td>BEDAZZLED MARRIED</td>
    </tr>
    <tr>
      <th>6</th>
      <td>BILKO ANONYMOUS</td>
    </tr>
    <tr>
      <th>7</th>
      <td>BLANKET BEVERLY</td>
    </tr>
    <tr>
      <th>8</th>
      <td>BLOOD ARGONAUTS</td>
    </tr>
    <tr>
      <th>9</th>
      <td>BLUES INSTINCT</td>
    </tr>
    <tr>
      <th>10</th>
      <td>BRAVEHEART HUMAN</td>
    </tr>
    <tr>
      <th>11</th>
      <td>CHASING FIGHT</td>
    </tr>
    <tr>
      <th>12</th>
      <td>CHISUM BEHAVIOR</td>
    </tr>
    <tr>
      <th>13</th>
      <td>CHOCOLAT HARRY</td>
    </tr>
    <tr>
      <th>14</th>
      <td>CONFUSED CANDLES</td>
    </tr>
    <tr>
      <th>15</th>
      <td>CONVERSATION DOWNHILL</td>
    </tr>
    <tr>
      <th>16</th>
      <td>DATE SPEED</td>
    </tr>
    <tr>
      <th>17</th>
      <td>DINOSAUR SECRETARY</td>
    </tr>
    <tr>
      <th>18</th>
      <td>DUMBO LUST</td>
    </tr>
    <tr>
      <th>19</th>
      <td>EARRING INSTINCT</td>
    </tr>
    <tr>
      <th>20</th>
      <td>EFFECT GLADIATOR</td>
    </tr>
    <tr>
      <th>21</th>
      <td>FEUD FROGMEN</td>
    </tr>
    <tr>
      <th>22</th>
      <td>FINDING ANACONDA</td>
    </tr>
    <tr>
      <th>23</th>
      <td>GABLES METROPOLIS</td>
    </tr>
    <tr>
      <th>24</th>
      <td>GANDHI KWAI</td>
    </tr>
    <tr>
      <th>25</th>
      <td>GLADIATOR WESTWARD</td>
    </tr>
    <tr>
      <th>26</th>
      <td>GREASE YOUTH</td>
    </tr>
    <tr>
      <th>27</th>
      <td>HALF OUTFIELD</td>
    </tr>
    <tr>
      <th>28</th>
      <td>HOCUS FRIDA</td>
    </tr>
    <tr>
      <th>29</th>
      <td>HOMICIDE PEACH</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
    </tr>
    <tr>
      <th>39</th>
      <td>MAGUIRE APACHE</td>
    </tr>
    <tr>
      <th>40</th>
      <td>MANCHURIAN CURTAIN</td>
    </tr>
    <tr>
      <th>41</th>
      <td>MOVIE SHAKESPEARE</td>
    </tr>
    <tr>
      <th>42</th>
      <td>MUSIC BOONDOCK</td>
    </tr>
    <tr>
      <th>43</th>
      <td>NATURAL STOCK</td>
    </tr>
    <tr>
      <th>44</th>
      <td>NETWORK PEAK</td>
    </tr>
    <tr>
      <th>45</th>
      <td>ODDS BOOGIE</td>
    </tr>
    <tr>
      <th>46</th>
      <td>OPPOSITE NECKLACE</td>
    </tr>
    <tr>
      <th>47</th>
      <td>PILOT HOOSIERS</td>
    </tr>
    <tr>
      <th>48</th>
      <td>PITTSBURGH HUNCHBACK</td>
    </tr>
    <tr>
      <th>49</th>
      <td>PRESIDENT BANG</td>
    </tr>
    <tr>
      <th>50</th>
      <td>PRIX UNDEFEATED</td>
    </tr>
    <tr>
      <th>51</th>
      <td>RAGE GAMES</td>
    </tr>
    <tr>
      <th>52</th>
      <td>RANGE MOONWALKER</td>
    </tr>
    <tr>
      <th>53</th>
      <td>REMEMBER DIARY</td>
    </tr>
    <tr>
      <th>54</th>
      <td>RESURRECTION SILVERADO</td>
    </tr>
    <tr>
      <th>55</th>
      <td>ROBBERY BRIGHT</td>
    </tr>
    <tr>
      <th>56</th>
      <td>RUSH GOODFELLAS</td>
    </tr>
    <tr>
      <th>57</th>
      <td>SECRETS PARADISE</td>
    </tr>
    <tr>
      <th>58</th>
      <td>SENSIBILITY REAR</td>
    </tr>
    <tr>
      <th>59</th>
      <td>SIEGE MADRE</td>
    </tr>
    <tr>
      <th>60</th>
      <td>SLUMS DUCK</td>
    </tr>
    <tr>
      <th>61</th>
      <td>SOUP WISDOM</td>
    </tr>
    <tr>
      <th>62</th>
      <td>SPARTACUS CHEAPER</td>
    </tr>
    <tr>
      <th>63</th>
      <td>SPINAL ROCKY</td>
    </tr>
    <tr>
      <th>64</th>
      <td>SPLASH GUMP</td>
    </tr>
    <tr>
      <th>65</th>
      <td>SUNSET RACER</td>
    </tr>
    <tr>
      <th>66</th>
      <td>SUPER WYOMING</td>
    </tr>
    <tr>
      <th>67</th>
      <td>VIRTUAL SPOILERS</td>
    </tr>
    <tr>
      <th>68</th>
      <td>WILLOW TRACY</td>
    </tr>
  </tbody>
</table>
<p>69 rows × 1 columns</p>
</div>




```python
#7.e
sql_query = """
select title, COUNT(rental.inventory_id) as "Rentals" from film
join inventory
on film.film_id = inventory.film_id
join rental
on rental. inventory_id = inventory.inventory_id
Group By film.film_id
Order by Rentals DESC;
"""
rental_amounts = pd.read_sql_query(sql_query, engine)
rental_amounts
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>title</th>
      <th>Rentals</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>BUCKET BROTHERHOOD</td>
      <td>34</td>
    </tr>
    <tr>
      <th>1</th>
      <td>ROCKETEER MOTHER</td>
      <td>33</td>
    </tr>
    <tr>
      <th>2</th>
      <td>RIDGEMONT SUBMARINE</td>
      <td>32</td>
    </tr>
    <tr>
      <th>3</th>
      <td>FORWARD TEMPLE</td>
      <td>32</td>
    </tr>
    <tr>
      <th>4</th>
      <td>JUGGLER HARDLY</td>
      <td>32</td>
    </tr>
    <tr>
      <th>5</th>
      <td>SCALAWAG DUCK</td>
      <td>32</td>
    </tr>
    <tr>
      <th>6</th>
      <td>GRIT CLOCKWORK</td>
      <td>32</td>
    </tr>
    <tr>
      <th>7</th>
      <td>APACHE DIVINE</td>
      <td>31</td>
    </tr>
    <tr>
      <th>8</th>
      <td>WIFE TURN</td>
      <td>31</td>
    </tr>
    <tr>
      <th>9</th>
      <td>TIMBERLAND SKY</td>
      <td>31</td>
    </tr>
    <tr>
      <th>10</th>
      <td>HOBBIT ALIEN</td>
      <td>31</td>
    </tr>
    <tr>
      <th>11</th>
      <td>ZORRO ARK</td>
      <td>31</td>
    </tr>
    <tr>
      <th>12</th>
      <td>ROBBERS JOON</td>
      <td>31</td>
    </tr>
    <tr>
      <th>13</th>
      <td>RUSH GOODFELLAS</td>
      <td>31</td>
    </tr>
    <tr>
      <th>14</th>
      <td>NETWORK PEAK</td>
      <td>31</td>
    </tr>
    <tr>
      <th>15</th>
      <td>GOODFELLAS SALUTE</td>
      <td>31</td>
    </tr>
    <tr>
      <th>16</th>
      <td>DOGMA FAMILY</td>
      <td>30</td>
    </tr>
    <tr>
      <th>17</th>
      <td>CAT CONEHEADS</td>
      <td>30</td>
    </tr>
    <tr>
      <th>18</th>
      <td>MARRIED GO</td>
      <td>30</td>
    </tr>
    <tr>
      <th>19</th>
      <td>FROST HEAD</td>
      <td>30</td>
    </tr>
    <tr>
      <th>20</th>
      <td>HARRY IDAHO</td>
      <td>30</td>
    </tr>
    <tr>
      <th>21</th>
      <td>WITCHES PANIC</td>
      <td>30</td>
    </tr>
    <tr>
      <th>22</th>
      <td>GRAFFITI LOVE</td>
      <td>30</td>
    </tr>
    <tr>
      <th>23</th>
      <td>MUSCLE BRIGHT</td>
      <td>30</td>
    </tr>
    <tr>
      <th>24</th>
      <td>SHOCK CABIN</td>
      <td>30</td>
    </tr>
    <tr>
      <th>25</th>
      <td>BUTTERFLY CHOCOLAT</td>
      <td>30</td>
    </tr>
    <tr>
      <th>26</th>
      <td>PULP BEVERLY</td>
      <td>30</td>
    </tr>
    <tr>
      <th>27</th>
      <td>ENGLISH BULWORTH</td>
      <td>30</td>
    </tr>
    <tr>
      <th>28</th>
      <td>MASSACRE USUAL</td>
      <td>30</td>
    </tr>
    <tr>
      <th>29</th>
      <td>IDOLS SNATCHERS</td>
      <td>30</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>928</th>
      <td>RUSHMORE MERMAID</td>
      <td>6</td>
    </tr>
    <tr>
      <th>929</th>
      <td>PHANTOM GLORY</td>
      <td>6</td>
    </tr>
    <tr>
      <th>930</th>
      <td>JERSEY SASSY</td>
      <td>6</td>
    </tr>
    <tr>
      <th>931</th>
      <td>TERMINATOR CLUB</td>
      <td>6</td>
    </tr>
    <tr>
      <th>932</th>
      <td>KILLER INNOCENT</td>
      <td>6</td>
    </tr>
    <tr>
      <th>933</th>
      <td>SIMON NORTH</td>
      <td>6</td>
    </tr>
    <tr>
      <th>934</th>
      <td>TEXAS WATCH</td>
      <td>6</td>
    </tr>
    <tr>
      <th>935</th>
      <td>DUCK RACER</td>
      <td>6</td>
    </tr>
    <tr>
      <th>936</th>
      <td>SCHOOL JACKET</td>
      <td>6</td>
    </tr>
    <tr>
      <th>937</th>
      <td>APOCALYPSE FLAMINGOS</td>
      <td>6</td>
    </tr>
    <tr>
      <th>938</th>
      <td>CASSIDY WYOMING</td>
      <td>6</td>
    </tr>
    <tr>
      <th>939</th>
      <td>WATERSHIP FRONTIER</td>
      <td>6</td>
    </tr>
    <tr>
      <th>940</th>
      <td>BUBBLE GROSSE</td>
      <td>6</td>
    </tr>
    <tr>
      <th>941</th>
      <td>MANNEQUIN WORST</td>
      <td>5</td>
    </tr>
    <tr>
      <th>942</th>
      <td>SEVEN SWARM</td>
      <td>5</td>
    </tr>
    <tr>
      <th>943</th>
      <td>FULL FLATLINERS</td>
      <td>5</td>
    </tr>
    <tr>
      <th>944</th>
      <td>BRAVEHEART HUMAN</td>
      <td>5</td>
    </tr>
    <tr>
      <th>945</th>
      <td>HUNTER ALTER</td>
      <td>5</td>
    </tr>
    <tr>
      <th>946</th>
      <td>PRIVATE DROP</td>
      <td>5</td>
    </tr>
    <tr>
      <th>947</th>
      <td>BUNCH MINDS</td>
      <td>5</td>
    </tr>
    <tr>
      <th>948</th>
      <td>MUSSOLINI SPOILERS</td>
      <td>5</td>
    </tr>
    <tr>
      <th>949</th>
      <td>FEVER EMPIRE</td>
      <td>5</td>
    </tr>
    <tr>
      <th>950</th>
      <td>FREEDOM CLEOPATRA</td>
      <td>5</td>
    </tr>
    <tr>
      <th>951</th>
      <td>INFORMER DOUBLE</td>
      <td>5</td>
    </tr>
    <tr>
      <th>952</th>
      <td>CONSPIRACY SPIRIT</td>
      <td>5</td>
    </tr>
    <tr>
      <th>953</th>
      <td>TRAFFIC HOBBIT</td>
      <td>5</td>
    </tr>
    <tr>
      <th>954</th>
      <td>GLORY TRACY</td>
      <td>5</td>
    </tr>
    <tr>
      <th>955</th>
      <td>TRAIN BUNCH</td>
      <td>4</td>
    </tr>
    <tr>
      <th>956</th>
      <td>HARDLY ROBBERS</td>
      <td>4</td>
    </tr>
    <tr>
      <th>957</th>
      <td>MIXED DOORS</td>
      <td>4</td>
    </tr>
  </tbody>
</table>
<p>958 rows × 2 columns</p>
</div>




```python
#7.f
sql_query = """
select store_id, SUM(amount) from store
join payment
on payment.staff_id = store.manager_staff_id
Group By store_id;
"""
store_totals = pd.read_sql_query(sql_query, engine)
store_totals
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>store_id</th>
      <th>SUM(amount)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>33489.47</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>33927.04</td>
    </tr>
  </tbody>
</table>
</div>




```python
#7.g
sql_query = """
select store_id, city, country 
from store
join address
on address.address_id = store.address_id
join city
on city.city_id = address.city_id
join country
on country.country_id = city.country_id;
"""
store_loc = pd.read_sql_query(sql_query, engine)
store_loc
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>store_id</th>
      <th>city</th>
      <th>country</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Lethbridge</td>
      <td>Canada</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>Woodridge</td>
      <td>Australia</td>
    </tr>
  </tbody>
</table>
</div>




```python
#7.h
sql_query = """
select name, SUM(amount) as Total from category
join film_category
on film_category.category_id = category.category_id
join inventory
on film_category.film_id = inventory.film_id
join rental
on inventory.inventory_id = rental.inventory_id
join payment
on payment.rental_id = rental.rental_id
Group BY category.name
Order BY Total Desc;
"""
cat_totals = pd.read_sql_query(sql_query, engine)
cat_totals.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>name</th>
      <th>Total</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Sports</td>
      <td>5314.21</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Sci-Fi</td>
      <td>4756.98</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Animation</td>
      <td>4656.30</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Drama</td>
      <td>4587.39</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Comedy</td>
      <td>4383.58</td>
    </tr>
  </tbody>
</table>
</div>




```python
#8.a
sql_query = """
create view top_five_genres as
select name, SUM(amount) as Total from category
join film_category
on film_category.category_id = category.category_id
join inventory
on film_category.film_id = inventory.film_id
join rental
on inventory.inventory_id = rental.inventory_id
join payment
on payment.rental_id = rental.rental_id
Group BY category.name
Order BY Total Desc;
"""

```


```python
#8.b
def RunSQL(sql_command):
    connection = pymysql.connect(host='localhost',
                             user='root',
                             password='n7tg541aw',
                             db='sakila',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            commands = sql_command.split(';')
            for command in commands:
                if command == '\n': continue
                cursor.execute(command + ';')
                connection.commit()
    except Exception as e: 
        print(e)
    finally:
        connection.close()
```


```python
#8.b
RunSQL(sql_query)
```


```python
#8.b
top_five = pd.read_sql_query('select * from top_five_genres', engine)
top_five.head()

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>name</th>
      <th>Total</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Sports</td>
      <td>5314.21</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Sci-Fi</td>
      <td>4756.98</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Animation</td>
      <td>4656.30</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Drama</td>
      <td>4587.39</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Comedy</td>
      <td>4383.58</td>
    </tr>
  </tbody>
</table>
</div>




```python
#8.c
sql_query = """
DROP VIEW top_five_genres;
"""
view_drop = pd.read_sql_query(sql_query, engine)

```


    ---------------------------------------------------------------------------

    AttributeError                            Traceback (most recent call last)

    ~\Anaconda3\envs\py36\lib\site-packages\sqlalchemy\engine\result.py in _fetchall_impl(self)
       1081         try:
    -> 1082             return self.cursor.fetchall()
       1083         except AttributeError:
    

    AttributeError: 'NoneType' object has no attribute 'fetchall'

    
    During handling of the above exception, another exception occurred:
    

    ResourceClosedError                       Traceback (most recent call last)

    <ipython-input-85-1a117711dfad> in <module>()
          3 DROP VIEW top_five_genres;
          4 """
    ----> 5 view_drop = pd.read_sql_query(sql_query, engine)
    

    ~\Anaconda3\envs\py36\lib\site-packages\pandas\io\sql.py in read_sql_query(sql, con, index_col, coerce_float, params, parse_dates, chunksize)
        330     return pandas_sql.read_query(
        331         sql, index_col=index_col, params=params, coerce_float=coerce_float,
    --> 332         parse_dates=parse_dates, chunksize=chunksize)
        333 
        334 
    

    ~\Anaconda3\envs\py36\lib\site-packages\pandas\io\sql.py in read_query(self, sql, index_col, coerce_float, parse_dates, params, chunksize)
       1099                                         parse_dates=parse_dates)
       1100         else:
    -> 1101             data = result.fetchall()
       1102             frame = _wrap_result(data, columns, index_col=index_col,
       1103                                  coerce_float=coerce_float,
    

    ~\Anaconda3\envs\py36\lib\site-packages\sqlalchemy\engine\result.py in fetchall(self)
       1135             self.connection._handle_dbapi_exception(
       1136                 e, None, None,
    -> 1137                 self.cursor, self.context)
       1138 
       1139     def fetchmany(self, size=None):
    

    ~\Anaconda3\envs\py36\lib\site-packages\sqlalchemy\engine\base.py in _handle_dbapi_exception(self, e, statement, parameters, cursor, context)
       1414                 )
       1415             else:
    -> 1416                 util.reraise(*exc_info)
       1417 
       1418         finally:
    

    ~\Anaconda3\envs\py36\lib\site-packages\sqlalchemy\util\compat.py in reraise(tp, value, tb, cause)
        185         if value.__traceback__ is not tb:
        186             raise value.with_traceback(tb)
    --> 187         raise value
        188 
        189 else:
    

    ~\Anaconda3\envs\py36\lib\site-packages\sqlalchemy\engine\result.py in fetchall(self)
       1129 
       1130         try:
    -> 1131             l = self.process_rows(self._fetchall_impl())
       1132             self._soft_close()
       1133             return l
    

    ~\Anaconda3\envs\py36\lib\site-packages\sqlalchemy\engine\result.py in _fetchall_impl(self)
       1082             return self.cursor.fetchall()
       1083         except AttributeError:
    -> 1084             return self._non_result([])
       1085 
       1086     def _non_result(self, default):
    

    ~\Anaconda3\envs\py36\lib\site-packages\sqlalchemy\engine\result.py in _non_result(self, default)
       1087         if self._metadata is None:
       1088             raise exc.ResourceClosedError(
    -> 1089                 "This result object does not return rows. "
       1090                 "It has been closed automatically.",
       1091             )
    

    ResourceClosedError: This result object does not return rows. It has been closed automatically.

