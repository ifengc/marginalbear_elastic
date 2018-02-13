from elasticsearch_dsl.connections import connections
from marginalbear_elastic.mapping import Post

connections.create_connection(hosts=['elastic:changeme@localhost'], timeout=20)
Post.init()
