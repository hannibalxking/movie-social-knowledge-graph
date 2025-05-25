## 🏰 The Quest Begins: The Movie & Social Knowledge Graph

Welcome, brave adventurer, to the realm of connected data and ancient wisdom stored in knowledge graphs. Embark on this epic journey as we unravel the mysteries of movies, people, characters, companies, reviews, and social links, all intertwined in a grand tapestry of information.

### 🛡️ Gathering Allies: Installation & Setup

Before we embark on our quest, we must gather our tools and forge our alliances. Ensure you have the following allies by your side:

1. **Neo4j v5+**: Install Neo4j locally (Community Edition or via Docker).
2. **Python 3.8+**: Set up a virtual environment with Python 3.8 or higher.
3. **Packages**: Install the necessary packages using the following incantation:

```bash
pip install neo4j pandas
```

### 🔮 Mastering the Ancient Arts: Usage

With our allies gathered, it's time to master the ancient arts of data ingestion and graph manipulation. Below are the mystic incantations and rituals you must perform to harness the power of the knowledge graph.

#### 🌐 Loading Static Domain Data

##### 🎭 Genres & Companies

```python
def load_genres_and_companies(tx):
    genres = ['Action','Sci-Fi','Thriller','Drama']
    for name in genres:
        tx.run("MERGE (:Genre {name:$name})", name=name)

    companies = [
        {"name":"Warner Bros.", "founded":1923, "country":"US"},
        {"name":"Paramount Pictures", "founded":1912, "country":"US"}
    ]
    for c in companies:
        tx.run("""
          MERGE (co:Company {name:$name})
          SET co.founded = $founded, co.country = $country
        """, **c)
```

##### 🎬 Movies & Genre Links

```python
def load_movies(tx):
    movies = [
        {"title":"Inception","released":2010,"tagline":"Your mind is the scene of the crime","genres":["Thriller","Sci-Fi"]},
        {"title":"Interstellar","released":2014,"tagline":"Mankind’s next step will be our greatest","genres":["Sci-Fi","Drama"]}
    ]
    for m in movies:
        tx.run("MERGE (mv:Movie {title:$title}) SET mv.released=$released, mv.tagline=$tagline", **m)
        for g in m["genres"]:
            tx.run("""
              MATCH (mv:Movie {title:$title}), (g:Genre {name:$genre})
              MERGE (mv)-[:IN_GENRE]->(g)
            """, title=m["title"], genre=g)
```

##### 🎭 Characters, Actors, Directors, Writers

```python
def load_people_and_roles(tx):
    characters = [
        {"name":"Cobb","movie":"Inception","archetype":"Hero"},
        {"name":"Murph","movie":"Interstellar","archetype":"Protege"}
    ]
    for ch in characters:
        tx.run("""
          MERGE (c:Character {name:$name, movie:$movie})
          SET c.archetype=$archetype
        """, **ch)

    actors = [
        {"name":"Leonardo DiCaprio","born":1974,"nationality":"US","character":"Cobb","year":2010},
        {"name":"Jessica Chastain","born":1977,"nationality":"US","character":"Murph","year":2014}
    ]
    for a in actors:
        tx.run("""
          MERGE (p:Person {name:$name})
          SET p.born=$born, p.nationality=$nationality
        """, **a)
        tx.run("""
          MATCH (p:Person {name:$name}), (c:Character {name:$character, movie:$character})
          MERGE (p)-[:ACTED_AS {roles:[$character], year:$year}]->(c)
        """, name=a["name"], character=a["character"], year=a["year"])

    directors = [
        {"director":"Christopher Nolan","movie":"Inception","year":2010},
        {"director":"Christopher Nolan","movie":"Interstellar","year":2014}
    ]
    for d in directors:
        tx.run("""
          MERGE (p:Person {name:$director})
          MERGE (m:Movie {title:$movie})
          MERGE (p)-[:DIRECTED {year:$year}]->(m)
        """, **d)
```

##### 📝 Reviews, Social Follows & Likes

```python
def load_reviews_and_social(tx):
    for u in ["Alice","Bob","Carol"]:
        tx.run("MERGE (:User {name:$name})", name=u)

    reviews = [
        {"user":"Alice","movie":"Inception","rating":5,"date":"2021-01-01","comment":"Mind-blowing!"},
        {"user":"Bob","movie":"Interstellar","rating":4,"date":"2021-02-02","comment":"Epic visuals."}
    ]
    for r in reviews:
        tx.run("""
          MATCH (u:User {name:$user}), (m:Movie {title:$movie})
          CREATE (rev:Review {rating:$rating, date:date($date), comment:$comment})
          MERGE (u)-[:WROTE]->(rev)
          MERGE (rev)-[:FOR_MOVIE]->(m)
        """, **r)

    follows = [("Alice","Bob","2021-03-01"),("Bob","Carol","2021-03-05")]
    for fr,to,date in follows:
        tx.run("""
          MATCH (a:User {name:$fr}), (b:User {name:$to})
          MERGE (a)-[f:FOLLOWS]->(b)
          ON CREATE SET f.since = date($date)
        """, fr=fr, to=to, date=date)
```

##### 📆 Temporal Releases & Versions

```python
def load_temporal_and_versions(tx):
    releases = [
        {"movie":"Inception","region":"US","date":"2010-07-16"},
        {"movie":"Inception","region":"FR","date":"2010-07-21"}
    ]
    for r in releases:
        tx.run("MERGE (rel:Release {region:$region, date:date($date)})", **r)
        tx.run("""
          MATCH (m:Movie {title:$movie}), (rel:Release {region:$region, date:date($date)})
          MERGE (m)-[:RELEASED_IN {region:$region, date:date($date)}]->(rel)
        """, **r)

    versions = [{"movie":"Interstellar","label":"4K Remaster","releaseDate":"2020-11-01"}]
    for v in versions:
        tx.run("""
          MERGE (ver:Version {label:$label})
          SET ver.releaseDate=date($releaseDate)
        """, **v)
        tx.run("""
          MATCH (m:Movie {title:$movie}), (ver:Version {label:$label})
          MERGE (m)-[:HAS_VERSION {releaseDate:date($releaseDate)}]->(ver)
        """, **v)
```

### 🔬 Graph Data Science: Unlocking Hidden Insights

With the knowledge graph constructed, it's time to unlock its hidden insights using the powerful spells of Graph Data Science.

#### 📊 PageRank on Social Graph

```python
def run_gds(tx):
    tx.run("CALL gds.graph.drop('social', false)").consume()
    tx.run("""
      CALL gds.graph.project('social','User',{FOLLOWS:{orientation:'NATURAL'}})
    """).consume()

    return tx.run("""
      CALL gds.pageRank.stream('social')
      YIELD nodeId, score
      RETURN gds.util.asNode(nodeId).name AS user, round(score,3) AS pr
      ORDER BY pr DESC
    """).data()
```

#### 🎞 Movie Similarity via Genres

```python
def run_movie_similarity(tx):
    tx.run("CALL gds.graph.drop('movieActor', false)").consume()
    tx.run("""
      CALL gds.graph.project.cypher(
        'movieActor',
        'MATCH (m:Movie) RETURN id(m) AS id',
        'MATCH (m1)<-[:IN_GENRE]-(:Genre)-[:IN_GENRE]->(m2)
         WHERE id(m1)<id(m2)
         RETURN id(m1) AS source, id(m2) AS target'
      )
    """).consume()

    return tx.run("""
      CALL gds.nodeSimilarity.stream('movieActor',{similarityCutoff:0.2})
      YIELD node1,node2,similarity
      RETURN gds.util.asNode(node1).title AS A,
             gds.util.asNode(node2).title AS B,
             round(similarity,3)             AS sim
      ORDER BY sim DESC LIMIT 5
    """).data()
```

### 🛡️ Putting It All Together: The Final Incantation

With all the pieces in place, it's time to cast the final incantation that brings everything together.

```python
def main():
    with driver.session() as s:
        s.execute_write(create_constraints_and_indexes)
        s.execute_write(load_genres_and_companies)
        s.execute_write(load_movies)
        s.execute_write(load_people_and_roles)
        s.execute_write(load_reviews_and_social)
        s.execute_write(load_temporal_and_versions)

        pr_scores = s.execute_read(run_gds)
        sim_pairs = s.execute_read(run_movie_similarity)

    import pandas as pd
    print("PageRank scores:\n", pd.DataFrame(pr_scores))
    print("\nTop movie similarities:\n", pd.DataFrame(sim_pairs))
    driver.close()

if __name__ == "__main__":
    main()
```

### 📜 Next Steps: Expanding the Realm

Your journey does not end here, brave adventurer. There are always more lands to explore and knowledge to uncover:

1. **Expose an API**: Create an API using Flask or FastAPI to run parameterized Cypher queries.
2. **Load Real Data**: Import real data from CSV files, TMDB, or Wikidata.
3. **Add neosemantics Plugin**: Export RDF/SPARQL using the neosemantics (n10s) plugin.
4. **Visualize**: Use Neo4j Bloom or Neodash to visualize your graph.

May your journey be filled with wisdom and discovery. Happy graphing!