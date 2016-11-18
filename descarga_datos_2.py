import pytumblr
from neo4jrestclient.client import GraphDatabase
from neo4jrestclient import client
from datetime import datetime
from time import ctime, sleep


dbp = GraphDatabase("http://localhost:7474", username="neo4j", password="Doraemon2")

cliente = pytumblr.TumblrRestClient(
  '22Uf8ZiKTkSOzlxsqLNtbM8r9Ey5rbixpZGyE94QoOUCpOOrHy',
  'nBeoEL8tOqtD1nUPYP1mkVGIdvEzsoQwxtzAFOKTbM1rKA8Yvq',
  'vdc1s0MGLPgfltVUTBtDJMKVCIC7ICYR0D2nhoeJKITQcMxqmg',
  '9G2K9tUn4KXP0Y6QsVQJOR9nYaQqOf5gMb2SmdEcWQHEVSfYKF'
)

'''tam = cliente.following()['total_blogs']

cola_fifo = []


for i in range(0, tam, 10):

    cola_fifo.extend([blog['name'] for blog in cliente.following(offset=i, limit=10)['blogs']])

f = open("ddp_log.txt", 'w')
f.writelines([blog+"\n" for blog in cola_fifo])
f.close()'''

'''PARA RECUPERAR'''
f = open("ddp_log.txt", "r")
cola_fifotexto = [line for line in f.readlines()]
cola_fifo = [blog for blog in map(lambda x: x.replace('\n', ''), cola_fifotexto) if blog != '']
f.close()

#Se crea la etiqueta Blog para la base de datos
blogp = dbp.labels.create("Blog")
postbd = dbp.labels.create("Post")

def extrae_blogs(blog):

    blogsextraidos = []

    url = ".tumblr.com"

    #Estos dos tipos porque son el porcentaje mas alto de posts en tumblr
    try:
        postsfotos = cliente.posts(blog+url, type="photo", limit=20, notes_info=True)['posts']
    except KeyError:
        return blogsextraidos
    poststexto = cliente.posts(blog+url, type="text", limit=20, notes_info=True)['posts']

    blogsextraidos.extend(list(set([post['trail'][0]['blog']['name']
                      for post in postsfotos
                      if post['trail'] and blog != post['trail'][0]['blog']['name']])))
    blogsextraidos.extend(list(set([post['trail'][0]['blog']['name']
                      for post in poststexto
                      if post['trail'] and blog != post['trail'][0]['blog']['name']])))

    return list(set(blogsextraidos)), postsfotos, poststexto

def new_blog(blog, princ=False):

    infoblog = cliente.blog_info(blog)

    try:
      link = infoblog['blog']['url']
    except KeyError:
      return "next"

    if princ:
        return "exists"
    else:
        posts = infoblog['blog']['total_posts']
        desc = infoblog['blog']['description']
        act = str(to_tiempolocal(infoblog['blog']['updated']))

        b = dbp.nodes.create(nombre=blog, url=link, numposts=posts,
                            bio=desc, actualizado=act)

        return b

def to_tiempolocal(secondsepoch):

    return datetime.strptime(ctime(secondsepoch), '%a %b %d %H:%M:%S %Y')

def crea_post(post):

    def comprueba_existe_post(idp):

        q = """match (n:Post {iden:'"""+idp+"""'}) return n, n.iden"""
        results = dbp.query(q, returns=(client.Node, unicode, client.Relationship))

        return False if not results else True

    iden = str(post['id'])

    if comprueba_existe_post(iden):
        return False

    tags = ','.join(post['tags'])
    fecha = str(to_tiempolocal(post['timestamp']))
    numlikes = str(post['note_count'])
    try:
        titulo = post['title']
    except KeyError:
        titulo = ""

    p = dbp.nodes.create(iden=iden, tags=tags, fecha=fecha, numlikes=numlikes, tit=titulo)

    return p

def crea_relaciones_post(post):

    try:
        notas = post['notes']
    except KeyError:
        notas = []

    idp = str(post['id'])
    q = """match (n:Post {iden:'"""+idp+"""'}) return n, n.iden"""
    postnodo = dbp.query(q, returns=(client.Node, unicode, client.Relationship))[0][0]

    for n in notas:

        q = """match (n:Blog {nombre:'"""+n['blog_name']+"""'}) return n, n.nombre"""
        res = dbp.query(q, returns=(client.Node, unicode, client.Relationship))

        if not res:
            blognodo = new_blog(n['blog_name'])
            blogp.add(blognodo)

        else:
            blognodo = res[0][0]


        if n['type'] == 'like':
            blognodo.relationships.create('likes', postnodo, since=to_tiempolocal(n['timestamp']))

        elif n['type'] == 'reblog':
            blognodo.relationships.create('reblog', postnodo, since=to_tiempolocal(n['timestamp']))

        else:
            pass

listanoexisten = []

while len(cola_fifo) > 0:

    print "-----------------------------------------------"
    blogactual = cola_fifo[0]
    print blogactual

    if blogactual in listanoexisten:
        blogsparciales = []
        cola_fifo.extend(blogsparciales)
        cola_fifo = cola_fifo[1:]
        f = open("ddp_log.txt", 'w')
        f.writelines([blog+"\n" for blog in cola_fifo])
        f.close()
        continue

    bp = new_blog(blogactual, princ=True)

    if bp == "next":
        print "El blog no existe"
        #Si el resultado es next, es que el blog ya no existe, y no se investigan sobre sus follows
        blogsparciales = []
        listanoexisten.append(blogactual)
    else:
        packposts = extrae_blogs(blogactual)
        blogsparciales = packposts[0]
        postsfotos = packposts[1]
        poststexto = packposts[2]

        #Para posts de imagenes
        for p in postsfotos:

            almacenp = crea_post(p)
            if not almacenp:
                break
            else:
                postbd.add(almacenp)
                crea_relaciones_post(p)

        #Para posts de texto
        for p in poststexto:

            almacenp = crea_post(p)
            if not almacenp:
                break
            else:
                postbd.add(almacenp)
                crea_relaciones_post(p)

    #Eliminar duplicados del blogsparciales
    cola_fifo.extend(blogsparciales)
    cola_fifo = cola_fifo[1:]
    f = open("ddp_log.txt", 'w')
    f.writelines([blog+"\n" for blog in cola_fifo])
    f.close()
    sleep(60)