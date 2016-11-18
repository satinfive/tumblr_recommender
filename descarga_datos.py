import pytumblr
from neo4jrestclient.client import GraphDatabase
from neo4jrestclient import client
from datetime import datetime
from time import ctime, sleep

db = GraphDatabase("http://localhost:7475", username="neo4j", password="Doraemon2")

# Authenticate via OAuth
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

f = open("ddf_log.txt", 'w')
f.writelines([blog+"\n" for blog in cola_fifo])
f.close()'''

'''PARA RECUPERAR'''
f = open("ddf_log.txt", "r")
cola_fifotexto = [line for line in f.readlines()]
cola_fifo = [blog for blog in map(lambda x: x.replace('\n', ''), cola_fifotexto) if blog != '']
f.close()


'''Funciones para la descarga de datos'''
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



'''Funciones para la base de datos'''
def new_blog(blog):

    infoblog = cliente.blog_info(blog)

    try:
      link = infoblog['blog']['url']
    except KeyError:
      return "next"

    posts = infoblog['blog']['total_posts']
    desc = infoblog['blog']['description']
    act = str(to_tiempolocal(infoblog['blog']['updated']))


    b = db.nodes.create(nombre=blog, url=link, numposts=posts,
                        bio=desc, actualizado=act)

    return b

def relacion_follower(blogprincipal, blogsec):

    blogprincipal.relationships.create("follows", blogsec)

def comprueba_blog_bd(blog):
    #Comprueba si el blog existe en la base de datos

    q = """match (n:Blog {nombre:'"""+blog+"""'}) return n, n.name"""
    results = db.query(q, returns=(client.Node, unicode, client.Relationship))

    return results if results else False

def comprueba_relacion(blog1, blog2):

    q = """MATCH (a:Blog)-[r:follows]-(b:Blog) where a.nombre='"""+blog1+"""' AND b.nombre='"""+blog2+"""' RETURN r"""
    results = db.query(q, returns=(client.Relationship))

    return results if results else False

def actualiza_datos(blognodo):
    #Actualiza los datos del nodo al volverselo encontrar y estar en la BD
    numpostsant = blognodo['numposts']
    bioant = blognodo['bio']
    updateant = blognodo['actualizado']

    nombreblog = blognodo['nombre']
    infoblog = cliente.blog_info(nombreblog)

    posts = infoblog['blog']['total_posts']
    desc = infoblog['blog']['description']
    act = str(to_tiempolocal(infoblog['blog']['updated']))

    blognodo['numposts'] = posts if posts != numpostsant else numpostsant
    blognodo['bio'] = desc if desc != bioant else bioant
    blognodo['actualizado'] = act if act != updateant else updateant

def to_tiempolocal(secondsepoch):

    return datetime.strptime(ctime(secondsepoch), '%a %b %d %H:%M:%S %Y')


#Se crea la etiqueta Blog para la base de datos
blog = db.labels.create("Blog")

'''Codigo principal para la descarga y almacenamiento de los datos en la BD'''
while len(cola_fifo) > 0:

    print "-----------------------------------------------"
    blogactual = cola_fifo[0]
    print blogactual

    existe = comprueba_blog_bd(blogactual)

    if not existe:
        print "Blog principal no esta en la BD"
        #En caso de que el blog actual no este en la base de datos

        bp = new_blog(blogactual)
        print bp

        if bp == "next":
            print "El blog principal no existe"
            #Si el resultado es next, es que el blog ya no existe, y no se investigan sobre sus follows
            blogsparciales = []

        else:
            blog.add(bp)
            packposts = extrae_blogs(blogactual)
            blogsparciales = packposts[0]

            for blogpost in blogsparciales:
                existeparc = comprueba_blog_bd(blogpost)
                print blogpost

                if not existeparc:
                    print "El blog secundario no esta"
                    b = new_blog(blogpost)
                    if b == 'next':
                        print "El blog secundario no existe"
                        continue
                    blog.add(b)
                    relacion_follower(bp, b)
                else:
                    print "El blog secundario esta"
                    b = existeparc[0][0]
                    actualiza_datos(b)
                    relacion_follower(bp, b)

    else:
        print "Blog principal esta en la BD"
        #En caso de que si este, hay que obtenerlo y crear las relaciones correspondientes

        packposts = extrae_blogs(blogactual)
        blogsparciales = packposts[0]

        bp = existe[0][0]
        actualiza_datos(bp)

        for blogpost in blogsparciales:

            print blogpost

            existerel = comprueba_relacion(blogactual, blogpost)
            if not existerel:
                print "No existe relacion entre blog principal y secundario"
                #La relacion no existe entre ambos
                existeparc = comprueba_blog_bd(blogpost)
                if not existeparc:
                    print "El blog secundario no esta"
                    #El blog parcial no existe en la bd, hay que ponerlo y crear la relacion
                    b = new_blog(blogpost)
                    if b == 'next':
                        print "El blog secundario no existe"
                        continue
                    blog.add(b)
                    relacion_follower(bp, b)
                else:
                    print "El blog secundario esta"
                    #El blog parcial existe, solo hay que crear la relacion
                    b = existeparc[0][0]
                    actualiza_datos(b)
                    relacion_follower(bp, b)
            else:
                print "Existe relacion entre blog principal y secundario"
                pass


    #Eliminar duplicados del blogsparciales
    cola_fifo.extend(blogsparciales)
    cola_fifo = cola_fifo[1:]
    f = open("ddf_log.txt", 'w')
    f.writelines([blog+"\n" for blog in cola_fifo])
    f.close()
    sleep(60)


#print client.blog_info('quietlim')
#TODO ESTO ES PARA LOS POSTS POR INDIVIDUAL
'''postsfotos = client.posts('satinfive', type="photo", limit=20, notes_info=True)['posts']
likes = postsfotos[5]['notes']
lik = []
reb = []
pos = []
for l in likes:

  if l['type'] == 'like':
    lik.append(l)

  if l['type'] == 'reblog':
    reb.append(l)

  if l['type'] == 'posted':
    pos.append(l)


print len(lik)
print len(reb)
print len(pos)
print likes'''

#COSAS PARA GUARDAR DE CADA POST:
#post['tags'], que te lo devuelve en lista

#post[date'] - que es el tiempo, no se si te lo devuelve en datetime
#post['note_count'] - que te lo devuelve en int

'''blog1 = 'nct-dream'
blog2 = 'nctlife'
q = """MATCH (a)-[r:follows]-(b) where a.nombre='"""+blog1+"""' AND b.nombre='"""+blog2+"""' RETURN r"""
results = db.query(q, returns=(client.Relationship))
rel = results[0][0]
print rel.start'''

#Hay que mejorar el codigo para que sea mas optimo
#HAY QUE SACAR A TODOS LOS FOLLOWINGS, QUE SOLO SALEN LOS 20