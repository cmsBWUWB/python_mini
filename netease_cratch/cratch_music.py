from selenium import webdriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import StaleElementReferenceException, NoSuchElementException
from selenium.common.exceptions import TimeoutException

from mysql.connector.errors import IntegrityError
import mysql.connector
import time
import thread
import threading


class Commentbean:
    comment_id, songid, songname, username, userhome, commentcontent, commenttime = 0, 0, 0, 0, 0, 0, 0


class Page:
    url = ""
    comment_count, title, current_page = 0, 0, 0
    driver, iframe, comment_box, comment_array, nextbt = 0, 0, 0, 0, 0

    def __init__(self, url: str):
        self.url = url

    def getpage(self):
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})
        cap = webdriver.DesiredCapabilities.CHROME
        cap.setdefault('chromeOptions', {'args': ['--headless', '--disable-gpu']})
        cap.setdefault('prefs', {"profile.managed_default_content_settings.images": 2})
        self.driver = webdriver.Remote("http://localhost:4444/wd/hub", cap)
        # self.driver = webdriver.Chrome(chrome_options=chrome_options)
        # self.driver = webdriver.PhantomJS()
        self.driver.set_page_load_timeout(5)
        try:
            self.driver.get(self.url)
        except TimeoutException:
            pass
        # 等待下方播放控件自动收起
        time.sleep(4)
        # self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight)");

        self.title = self.driver.find_element_by_xpath('/html[1]/head[1]/title[1]').get_attribute("textContent")
        self.driver.switch_to.default_content()
        self.iframe = self.driver.find_element_by_xpath('//iframe[@id="g_iframe"]')
        self.driver.switch_to.frame(self.iframe)
        self.comment_box = self.driver.find_element_by_xpath(".//div[@id='comment-box']")

    def getcommentdiv(self):
        self.comment_array = self.comment_box.find_elements(By.XPATH, ".//div[@class='m-cmmt']/div[contains(@class,'cmmts')]/div")
        self.nextbt = self.comment_box.find_element(By.XPATH, ".//div[@class='m-cmmt']/div[3]/div[1]/a[last()]")
        self.comment_count = self.comment_box.find_element_by_xpath("./div[1]/div[1]/span[1]/span[1]").text
        self.current_page = int(self.comment_box.find_element(By.XPATH,
                                               ".//div[@class='m-cmmt']/div[3]/div[1]/a[contains(@class,'js-selected')]").text)

    def jump(self, pageindex):
        pagebt = self.comment_box.find_element(By.XPATH, ".//div[@class='m-cmmt']/div[3]/div[1]/a[3]")
        btid = str(pagebt.get_attribute('id'))
        self.driver.execute_script("document.getElementById(\"" + btid + "\").innerHTML = \"" + str(pageindex) + "\";")
        pagebt.click()


def getcommentbean(comment: WebElement, commentbean: Commentbean):
    commentbean.comment_id = str(comment.get_attribute("data-id"))
    userdiv = comment.find_element(By.XPATH, "./div[2]/div[1]/div[1]/a[1]")
    commentbean.username = str(userdiv.text)
    commentbean.userhome = str(userdiv.get_attribute("href"))
    commentcontent = str(comment.find_element(By.XPATH, "./div[2]/div[1]/div[1]").text)
    commentbean.commentcontent = commentcontent[commentcontent.index(commentbean.username) + len(commentbean.username) + 1:]
    commentbean.commenttime = str(comment.find_element(By.XPATH, "./div[2]/div[last()]/div[1]").text)


class Manalyzediv(threading.Thread):
    def __init__(self, commentdiv, commentbean):
        threading.Thread.__init__(self)
        self.commentdiv = commentdiv
        self.commentbean = commentbean

    def run(self):
        getcommentbean(self.commentdiv, self.commentbean)
        global lock
        lock.acquire()
        insertdb(self.commentbean)
        lock.release()


def capturecomments(songid: str, pagebegin: int, pagecount: int):
    page = Page("https://music.163.com/#/song?id=" + songid)
    page.getpage()
    page.getcommentdiv()
    page.jump(pagebegin)
    while True:
        while True:
            try:
                commentbean = Commentbean()
                commentbean.songname = page.title
                commentbean.songid = songid
                getcommentbean(page.comment_array[len(page.comment_array) - 1], commentbean)
                insertdb(commentbean)
                break
            except (StaleElementReferenceException, NoSuchElementException):
                # print("comments are loading...")
                time.sleep(0.3)
                page.getcommentdiv()
                continue
        for i in range(len(page.comment_array)):
            commentbean = Commentbean()
            commentbean.songname = page.title
            commentbean.songid = songid
            getcommentbean(page.comment_array[i], commentbean)
            insertdb(commentbean)
        global lock
        lock.acquire()
        conn.commit()
        lock.release()
        print("page " + str(page.current_page) + " is done. start next page...")
        islastpage = "js-disabled" in page.nextbt.get_attribute("class")
        if islastpage or page.current_page > pagebegin + pagecount:
            print("this song is done.")
            page.driver.close()
            break
        page.nextbt.click()


def insertdb(commentbean: Commentbean):
    # print("insert...")
    global cursor, lock
    try:
        lock.acquire()
        # print("get lock")
        cursor.execute(insert_statement, (commentbean.comment_id, commentbean.songid, commentbean.songname,
                                          commentbean.userhome, commentbean.username, commentbean.commentcontent,
                                          commentbean.commenttime))
    except IntegrityError:
        pass
    finally:
        # print("release lock")
        lock.release()
    # print("complete...")


def test():
    while True:
        print("aaa")


conn = mysql.connector.connect(user='root', database='netease_music', password='root', use_unicode=True)
cursor = conn.cursor(buffered=True)
cursor.execute('SET NAMES utf8mb4')
cursor.execute("SET CHARACTER SET utf8mb4")
cursor.execute("SET character_set_connection=utf8mb4")

insert_statement = ("insert into comment (commentid, songid, song, userhome, username, comments_content, time) values (%s, %s, %s, %s, %s, %s, %s)")
# 28754103 85491
lock = threading.Lock()
done = 0

# capturecomments('85491', 1 + 50 * 0, 50)
for i in range(25):
    thread.start_new_thread(capturecomments, ('85491', 1 + 50 * i, 50))
    time.sleep(4)
while True:
    time.sleep(100)

# thread.start_new_thread(test, ())
conn.commit()
conn.close()
# while True:
#     thread.start_new_thread(test, ())
#     # time.sleep(0.1)
