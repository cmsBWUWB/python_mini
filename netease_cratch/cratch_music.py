from selenium import webdriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import StaleElementReferenceException, NoSuchElementException
from selenium.common.exceptions import TimeoutException
from lxml import etree
from io import StringIO

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
    driver, iframe, nextbt = 0, 0, 0
    parser = etree.HTMLParser()
    comment_array, comment_box = 0, 0
    tree = 0

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
        self.driver.set_page_load_timeout(10)
        try:
            self.driver.get(self.url)
        except TimeoutException:
            pass
        # 等待下方播放控件自动收起
        time.sleep(4)
        # self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight)");

        self.driver.switch_to.default_content()
        self.tree = etree.fromstring(self.driver.page_source, self.parser)
        self.title = self.tree.xpath("/html[1]/head[1]/title[1]")[0].text
        self.iframe = self.driver.find_element_by_xpath('//iframe[@id="g_iframe"]')

        self.driver.switch_to.frame(self.iframe)
        self.tree = etree.fromstring(self.driver.page_source, self.parser)
        self.comment_count = self.tree.xpath(".//div[@id='comment-box']/div[1]/div[1]/span[1]/span[1]")[0].text
        self.nextbt = self.driver.find_element_by_xpath(
            ".//div[@id='comment-box']//div[@class='m-cmmt']/div[3]/div[1]/a[last()]")

    def loadcomment(self):
        self.tree = etree.fromstring(self.driver.page_source, self.parser)
        self.comment_array = self.tree.xpath(".//div[@id='comment-box']//div[@class='m-cmmt']/div[contains(@class,'cmmts')]/div")
        self.current_page = int(self.tree.xpath(
            ".//div[@id='comment-box']//div[@class='m-cmmt']/div[3]/div[1]/a[contains(@class,'js-selected')]")[0].text)

    def jump(self, pageindex):
        pagebt = self.driver.find_element_by_xpath(".//div[@id='comment-box']//div[@class='m-cmmt']/div[3]/div[1]/a[3]")
        btid = str(pagebt.get_attribute('id'))
        self.driver.execute_script("document.getElementById(\"" + btid + "\").innerHTML = \"" + str(pageindex) + "\";")
        pagebt.click()


def getcommentbean(comment, commentbean: Commentbean):
    commentbean.comment_id = str(comment.get("data-id"))
    userdiv = comment.xpath("./div[2]/div[1]/div[1]/a[1]")[0]
    commentbean.username = str(userdiv.text)
    commentbean.userhome = str(userdiv.get("href"))
    temp = userdiv
    if temp.tail is None:
        temp = temp.getnext()
    commentcontent = str(temp.tail)[1:]
    temp = temp.getnext()
    while temp is not None:
        commentcontent += "[emoji]"
        if temp.tail is not None:
            commentcontent += str(temp.tail)
        temp = temp.getnext()
    commentbean.commentcontent = commentcontent
    commentbean.commenttime = str(comment.xpath("./div[2]/div[last()]/div[1]")[0].text)


def capturecomments(songid: str, pagebegin: int, pagecount: int):
    page = Page("https://music.163.com/#/song?id=" + songid)
    page.getpage()
    page.jump(pagebegin)
    while True:
        try:
            #判断是否加载完成
            WebDriverWait(page.driver, 10).until(
                expected_conditions.presence_of_element_located((By.XPATH,
                                                                "//div[@id='comment-box']//div[@class='m-cmmt']/div[contains(@class,'cmmts')]/div[last()]/div[2]/div[1]/div[1]/a[1]")))
        except TimeoutException:
            print("load comments timeout")
            page.driver.close()
            return
        page.loadcomment()
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
        if islastpage or page.current_page >= pagebegin + pagecount - 1:
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


class MyThread(threading.Thread):
    songid = 0
    begin, count = 0, 0
    semaphore = 0

    def __init__(self, songid: str, begin: int, count: int, semaphore: threading.Semaphore):
        threading.Thread.__init__(self)
        self.songid = songid
        self.begin = begin
        self.count = count
        self.semaphore = semaphore

    def run(self):
        self.semaphore.acquire()
        capturecomments(self.songid, self.begin, self.count)
        self.semaphore.release()



conn = mysql.connector.connect(user='root', database='netease_music', password='root', use_unicode=True)
cursor = conn.cursor(buffered=True)
cursor.execute('SET NAMES utf8mb4')
cursor.execute("SET CHARACTER SET utf8mb4")
cursor.execute("SET character_set_connection=utf8mb4")

insert_statement = ("insert into comment (commentid, songid, song, userhome, username, comments_content, time) values (%s, %s, %s, %s, %s, %s, %s)")
# 28754103 85491

threadcount = 10
songid = '85491'

page = Page("https://music.163.com/#/song?id=" + songid)
page.getpage()
pagecount = int(page.driver.find_element_by_xpath(".//div[@id='comment-box']//div[@class='m-cmmt']/div[3]/div[1]/a[last() - 1]").text)#总页数
page.driver.close()

lock = threading.Lock()
semaphore = threading.Semaphore(threadcount)
threadlist = []
everycount = int(pagecount / threadcount)
for i in range(int((pagecount - 1) / everycount) + 1):
    t = MyThread(songid, everycount * i + 1, everycount, semaphore)
    t.start()
    threadlist.append(t)
for t in threadlist:
    t.join()

conn.commit()
conn.close()
