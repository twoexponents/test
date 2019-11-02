import pymysql
import operator

def sql_connect():
  conn = pymysql.connect(host="localhost", user="root", passwd="mmlab2019", db="moloco")
  cursor = conn.cursor()
  return conn, cursor


def sql_close(cursor, conn):
  cursor.close()
  conn.close()


if __name__ == '__main__':
  conn, cursor, = sql_connect()

  sql = """
    SELECT site_id, count(distinct(user_id))
    FROM data
    WHERE user_id in (
        SELECT user_id
        FROM data
        GROUP BY user_id
        HAVING count(distinct(country_id)) > 1)
    GROUP BY site_id
    """
  cursor.execute(sql)
  rs = cursor.fetchall()

  sites = {}
  for item in rs:
    site_id, A_cnt, = item
    sites[site_id] = {}
    sites[site_id]['A'] = A_cnt

  sql = """
    SELECT site_id, count(distinct(user_id))
    FROM data
    GROUP BY site_id
    """
  cursor.execute(sql)
  rs = cursor.fetchall()

  for item in rs:
    site_id, B_counts, = item
    if site_id in sites:
      sites[site_id]['B'] = B_counts

  list = []
  for item in sites.keys():
    list.append((item, sites[item]['A'] / sites[item]['B']))
  list.sort(key = operator.itemgetter(1))
  list.reverse()
  print (list[:3])


  sql_close(cursor, conn)

