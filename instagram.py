from time import sleep

from instagram_web_api.client import Client as ClientWeb
from instagram_private_api.client import Client
from instagram_private_api.errors import ClientThrottledError
from data_access.pg_data_access import PgDataAccess, PgDbParams
from settings import SLEEP, PG, USER, PASS, SEARCH
from time import time


class InstagramCounter:

    def __init__(self, user_name: str, password: str, tag_search: str, params: dict):
        try:
            self.instagram_client = Client(user_name, password)
            self.tag = tag_search
            self.token = self.instagram_client.generate_uuid()
            self.db_params = PgDbParams(params)
            self.db_access = PgDataAccess(self.db_params)
            self.tag_id = self.save_tag()
        except Exception as ex:
            raise ex

    def get_items(self):
        result = self.instagram_client.feed_tag(self.tag, self.token)

        if result["status"] == "ok":
            return sorted(result["items"], key=lambda k: k['pk'], reverse=True)
        else:
            print(result)
            return []

    def get_last_id_item_save(self):
        query = """SELECT MAX(id) FROM ig_instagram_posts"""
        return self.db_access.execute_query_with_return(query)[0][0]

    def save_new_post(self, post):
        query_1 = f"""INSERT INTO "ig_instagram_users"("id","username","user_photo","insertion_timestamp") 
                     VALUES ( {post["user"]["pk"]}, '{post["user"]["username"]}', '{post["user"]["profile_pic_url"]}', CURRENT_DATE);"""
        query_2 = f"""INSERT INTO "ig_instagram_posts"("id","text","owner_id","is_video","media_url","post_date","insertion_timestamp") 
        VALUES ({post["pk"]},$escape${post["caption"]["text"]}$escape$,    {post["user"]["pk"]},    FALSE ,    '{post["image_versions2"]["candidates"][0]["url"] if "image_versions2" in post.keys() else ''}',
                {"TO_TIMESTAMP('"+str(post["taken_at"])+"')" if "taken_at" in post.keys() else "CURRENT_DATE"},    CURRENT_DATE );"""

        if 0 == self.db_access.execute_query_with_return("SELECT count(*) from ig_instagram_users where id=" + str(post["user"]["pk"]))[0][0]:
            self.db_access.execute_query(query_1)
        self.db_access.execute_query(query_2)
        self.db_access.execute_query(f"""INSERT INTO "public"."ig_instagram_posts_hashtags" ( "hashtag_id", "post_id") VALUES ( {self.tag_id}, {post["pk"]});""")

    def save_tag(self):
        result = self.db_access.execute_query_with_return(f"select id from ig_instagram_hashtags where text='{self.tag}'")
        if not result:
            self.db_access.execute_query(f"""INSERT INTO "public"."ig_instagram_hashtags" ( "id", "text") VALUES ( (select count(*)+1 from ig_instagram_hashtags), '{self.tag}');""")
            result = self.db_access.execute_query_with_return(f"select id from ig_instagram_hashtags where text='{self.tag}'")
        return result[0][0]

    def run(self):
        time_run = time()
        print("run extractor time")
        last_save = self.get_last_id_item_save()
        aux = 0

        if last_save is None:
            last_save = 0
        while True:

            print(f"start new branch seconds running: {time()-time_run}")
            items = self.get_items()
            for itm in items:
                if itm["pk"] > last_save:
                    if aux < itm["pk"]:
                        aux = itm["pk"]
                    try:
                        self.save_new_post(itm)
                    except Exception as ex:
                        print(str(ex))
                        continue
                    print(f"commit 1 post seconds running: {time()-time_run}")
                else:
                    break
            if aux > last_save:
                last_save = aux
            print(f"end branch seconds running: {time()-time_run}")
            print("sleep")
            sleep(SLEEP)


if __name__ == "__main__":
    print("start process")
    ig = InstagramCounter(USER, PASS, SEARCH, PG)
    print("done created class")
    ig.run()
