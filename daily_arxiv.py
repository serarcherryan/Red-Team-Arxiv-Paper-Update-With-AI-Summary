import os
import re
import json
import arxiv
import yaml
import logging
import argparse
import datetime
import requests
import time
from pathlib import Path
try:
    from openai import OpenAI  # For qwen-long via DashScope compatible API
except Exception:
    OpenAI = None
from requests.exceptions import SSLError, RequestException, ConnectionError, Timeout
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(format='[%(asctime)s %(levelname)s] %(message)s',
                    datefmt='%m/%d/%Y %H:%M:%S',
                    level=logging.INFO)

base_url = "https://arxiv.paperswithcode.com/api/v0/papers/"
github_url = "https://api.github.com/search/repositories"
arxiv_url = "http://arxiv.org/"

# Create a shared requests Session with retries for robustness
session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)


def get_json_with_retries(url: str, timeout_seconds: int = 10):
    """Fetch JSON with retries and graceful handling of SSL errors.

    Returns parsed JSON dict on success, or None on failure.
    """
    for attempt in range(1, 4):
        try:
            resp = session.get(url, timeout=timeout_seconds)
            resp.raise_for_status()
            return resp.json()
        except SSLError as e:
            logging.warning(f"SSL error on attempt {attempt} for {url}: {e}")
        except (ConnectionError, Timeout) as e:
            logging.warning(f"Connection/timeout on attempt {attempt} for {url}: {e}")
        except RequestException as e:
            logging.warning(f"Request error on attempt {attempt} for {url}: {e}")
        time.sleep(0.5 * attempt)
    return None


def ensure_dir(path: str) -> None:
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)


def sanitize_filename(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "_", name)


def download_pdf_for_paper(paper_key: str, dest_dir: str = "papers") -> str | None:
    """Download arXiv PDF for given paper key (e.g., 2508.17739) to dest_dir.
    Returns local file path or None on failure.
    """
    ensure_dir(dest_dir)
    pdf_url = f"https://arxiv.org/pdf/{paper_key}.pdf"
    local_path = os.path.join(dest_dir, f"{sanitize_filename(paper_key)}.pdf")
    try:
        resp = session.get(pdf_url, timeout=20)
        resp.raise_for_status()
        with open(local_path, "wb") as f:
            f.write(resp.content)
        logging.info(f"Downloaded PDF for {paper_key} -> {local_path}")
        return local_path
    except Exception as e:
        logging.warning(f"Failed to download PDF for {paper_key} from {pdf_url}: {e}")
        return None


def summarize_pdf_with_qwen_long(pdf_path: str) -> str | None:
    """Summarize the PDF using qwen-long via DashScope-compatible OpenAI client.
    Reads API key from DASHSCOPE_API_KEY if available. Returns response dict or None.
    """
    if OpenAI is None:
        logging.warning("openai package not available; skipping summarization")
        return None
    api_key = os.getenv("DASHSCOPE_API_KEY")
    if not api_key:
        logging.warning("DASHSCOPE_API_KEY not set; skipping summarization")
        return None
    try:
        client = OpenAI(api_key=api_key, base_url="https://dashscope.aliyuncs.com/compatible-mode/v1")
        file_object = client.files.create(file=Path(pdf_path), purpose="file-extract")
        completion = client.chat.completions.create(
            model="qwen-long",
            messages=[
                {"role": "system", "content": f"fileid://{file_object.id}"},
                {"role": "user", "content": "你是论文的作者，请用中文总结这篇论文的主要内容，并给出论文的结论。最终的输出格式为：'**论文主要内容**：[论文主要内容] <br><br> **论文结论**：[论文结论]'。你只需要填写[]里的内容，保留<br>，输出结果不要有任何换行行为。"},
            ],
        )
        logging.info(f"Summarization complete for {pdf_path}")
        # Extract text content robustly across return formats
        try:
            text = None
            # Preferred: object with .choices
            if hasattr(completion, "choices"):
                try:
                    text = completion.choices[0].message.get("content")
                except Exception:
                    text = None
            # Fallback: JSON string via model_dump_json
            if text is None and hasattr(completion, "model_dump_json"):
                try:
                    comp_json = json.loads(completion.model_dump_json())
                    text = comp_json.get("choices", [{}])[0].get("message", {}).get("content")
                except Exception as e :
                    text = None
            # Fallback: already a dict-like
            if text is None and isinstance(completion, dict):
                text = completion.get("choices", [{}])[0].get("message", {}).get("content")
            if not text:
                raise ValueError("empty content from completion")
            logging.info(f"Summarization text extracted: {text}")
            return text
        except Exception as e:
            logging.warning(f"Summarization failed for {pdf_path}: {e}")
        return None
    except Exception as e:
        logging.warning(f"Summarization failed for {pdf_path}: {e}")
        return None

def load_config(config_file:str) -> dict:
    '''
    config_file: input config file path
    return: a dict of configuration
    '''
    # make filters pretty
    def pretty_filters(**config) -> dict:
        keywords = dict()
        EXCAPE = '\"'
        QUOTA = '' # NO-USE
        OR = ' OR ' # TODO
        def parse_filters(filters:list):
            ret = ''
            for idx in range(0,len(filters)):
                filter = filters[idx]
                if len(filter.split()) > 1:
                    ret += (EXCAPE + filter + EXCAPE)
                else:
                    ret += (QUOTA + filter + QUOTA)
                if idx != len(filters) - 1:
                    ret += OR
            return ret
        for k,v in config['keywords'].items():
            keywords[k] = parse_filters(v['filters'])
        return keywords
    with open(config_file,'r', encoding='utf-8') as f:
        config = yaml.load(f,Loader=yaml.FullLoader)
        config['kv'] = pretty_filters(**config)
        logging.info(f'config = {config}')
    return config

def get_authors(authors, first_author = False):
    output = str()
    if first_author == False:
        output = ", ".join(str(author) for author in authors)
    else:
        output = authors[0]
    return output
def sort_papers(papers):
    output = dict()
    keys = list(papers.keys())
    keys.sort(reverse=True)
    for key in keys:
        output[key] = papers[key]
    return output
import requests

def get_code_link(qword:str) -> str:
    """
    This short function was auto-generated by ChatGPT.
    I only renamed some params and added some comments.
    @param qword: query string, eg. arxiv ids and paper titles
    @return paper_code in github: string, if not found, return None
    """
    # query = f"arxiv:{arxiv_id}"
    query = f"{qword}"
    params = {
        "q": query,
        "sort": "stars",
        "order": "desc"
    }
    r = requests.get(github_url, params=params)
    results = r.json()
    code_link = None
    if results["total_count"] > 0:
        code_link = results["items"][0]["html_url"]
    return code_link

def get_daily_papers(topic,query="slam", max_results=2):
    """
    @param topic: str
    @param query: str
    @return paper_with_code: dict
    """
    # output
    content = dict()
    content_to_web = dict()
    search_engine = arxiv.Search(
        query = query,
        max_results = max_results,
        sort_by = arxiv.SortCriterion.SubmittedDate
    )

    try:
        for result in search_engine.results():
            
            paper_id            = result.get_short_id()
            paper_title         = result.title
            paper_url           = result.entry_id
            code_url            = base_url + paper_id #TODO
            paper_abstract      = result.summary.replace("\n"," ")
            paper_authors       = get_authors(result.authors)
            paper_first_author  = get_authors(result.authors,first_author = True)
            primary_category    = result.primary_category
            publish_time        = result.published.date()
            update_time         = result.updated.date()
            comments            = result.comment

            logging.info(f"Time = {publish_time} title = {paper_title} author = {paper_first_author}")

            # Only process papers published today
            # if publish_time != datetime.date.today():
            # FIXME
            if publish_time != datetime.date.today() and publish_time != datetime.date(2025,8,25) and publish_time != datetime.date(2025,8,24):
                break

            # eg: 2108.09112v1 -> 2108.09112
            ver_pos = paper_id.find('v')
            if ver_pos == -1:
                paper_key = paper_id
            else:
                paper_key = paper_id[0:ver_pos]
            paper_url = arxiv_url + 'abs/' + paper_key

            try:
                # 1) Download today's paper PDF
                pdf_local_path = download_pdf_for_paper(paper_key)
                # 2) Summarize with qwen-long if configured
                summary_text = None
                if pdf_local_path:
                    summary_text = summarize_pdf_with_qwen_long(pdf_local_path)

                # source code link
                r = get_json_with_retries(code_url)
                repo_url = None
                if r and "official" in r and r["official"]:
                    repo_url = r["official"]["url"]
                # TODO: not found, two more chances
                # else:
                #    repo_url = get_code_link(paper_title)
                #    if repo_url is None:
                #        repo_url = get_code_link(paper_key)
                if repo_url is not None:
                    title_cell = paper_title
                    if summary_text:
                        title_cell = f"**{paper_title}**<br><br>{summary_text}"
                    else:
                        title_cell = f"**{paper_title}**"
                    content[paper_key] = "|**{}**|{}|{} et.al.|[{}]({})|**[link]({})**|\n".format(
                           publish_time,title_cell,paper_first_author,paper_key,paper_url,repo_url)
                    content_to_web[paper_key] = "- {}, **{}**, {} et.al., Paper: [{}]({}), Code: **[{}]({})**".format(
                           publish_time,paper_title,paper_first_author,paper_url,paper_url,repo_url,repo_url)

                else:
                    title_cell = paper_title
                    if summary_text:
                        title_cell = f"**{paper_title}**<br><br>{summary_text}"
                    else:
                        title_cell = f"**{paper_title}**"
                    content[paper_key] = "|**{}**|{}|{} et.al.|[{}]({})|null|\n".format(
                           publish_time,title_cell,paper_first_author,paper_key,paper_url)
                    content_to_web[paper_key] = "- {}, **{}**, {} et.al., Paper: [{}]({})".format(
                           publish_time,paper_title,paper_first_author,paper_url,paper_url)

                # TODO: select useful comments
                comments = None
                if comments != None:
                    content_to_web[paper_key] += f", {comments}\n"
                else:
                    content_to_web[paper_key] += f"\n"

            except Exception as e:
                logging.warning(f"Fetch code link failed: {e} with id: {paper_key}")
    except arxiv.UnexpectedEmptyPageError as e:
        logging.warning(f"arXiv returned an unexpected empty page; continuing with collected results. Details: {e}")

    data = {topic:content}
    data_web = {topic:content_to_web}
    return data,data_web

def update_paper_links(filename):
    '''
    weekly update paper links in json file
    '''
    def parse_arxiv_string(s):
        parts = s.split("|")
        date = parts[1].strip()
        title = parts[2].strip()
        authors = parts[3].strip()
        arxiv_id = parts[4].strip()
        code = parts[5].strip()
        arxiv_id = re.sub(r'v\d+', '', arxiv_id)
        return date,title,authors,arxiv_id,code

    with open(filename,"r", encoding='utf-8') as f:
        content = f.read()
        if not content:
            m = {}
        else:
            m = json.loads(content)

        json_data = m.copy()

        for keywords,v in json_data.items():
            logging.info(f'keywords = {keywords}')
            for paper_id,contents in v.items():
                contents = str(contents)

                update_time, paper_title, paper_first_author, paper_url, code_url = parse_arxiv_string(contents)

                contents = "|{}|{}|{}|{}|{}|\n".format(update_time,paper_title,paper_first_author,paper_url,code_url)
                json_data[keywords][paper_id] = str(contents)
                logging.info(f'paper_id = {paper_id}, contents = {contents}')

                valid_link = False if '|null|' in contents else True
                if valid_link:
                    continue
                try:
                    code_url = base_url + paper_id  # TODO
                    r = get_json_with_retries(code_url)
                    repo_url = None
                    if r and "official" in r and r["official"]:
                        repo_url = r["official"]["url"]
                        if repo_url is not None:
                            new_cont = contents.replace('|null|', f'|**[link]({repo_url})**|')
                            logging.info(f'ID = {paper_id}, contents = {new_cont}')
                            json_data[keywords][paper_id] = str(new_cont)

                except Exception as e:
                    logging.warning(f"Update code link failed: {e} with id: {paper_id}")
        # dump to json file
        with open(filename,"w", encoding='utf-8') as f:
            json.dump(json_data,f, ensure_ascii=False)

def update_json_file(filename,data_dict):
    '''
    daily update json file using data_dict
    '''
    with open(filename,"r", encoding='utf-8') as f:
        content = f.read()
        if not content:
            m = {}
        else:
            m = json.loads(content)

    json_data = m.copy()

    # update papers in each keywords
    for data in data_dict:
        for keyword in data.keys():
            papers = data[keyword]

            if keyword in json_data.keys():
                json_data[keyword].update(papers)
            else:
                json_data[keyword] = papers

    with open(filename,"w", encoding='utf-8') as f:
        json.dump(json_data,f, ensure_ascii=False)

def json_to_md(filename,md_filename,
               task = '',
               to_web = False,
               use_title = True,
               use_tc = True,
               show_badge = True,
               use_b2t = True):
    """
    @param filename: str
    @param md_filename: str
    @return None
    """
    def pretty_math(s:str) -> str:
        ret = ''
        match = re.search(r"\$.*\$", s)
        if match == None:
            return s
        math_start,math_end = match.span()
        space_trail = space_leading = ''
        if s[:math_start][-1] != ' ' and '*' != s[:math_start][-1]: space_trail = ' '
        if s[math_end:][0] != ' ' and '*' != s[math_end:][0]: space_leading = ' '
        ret += s[:math_start]
        ret += f'{space_trail}${match.group()[1:-1].strip()}${space_leading}'
        ret += s[math_end:]
        return ret

    DateNow = datetime.date.today()
    DateNow = str(DateNow)
    DateNow = DateNow.replace('-','.')

    with open(filename,"r", encoding='utf-8') as f:
        content = f.read()
        if not content:
            data = {}
        else:
            data = json.loads(content)

    # clean README.md if daily already exist else create it
    with open(md_filename,"w+", encoding='utf-8') as f:
        pass

    # write data into README.md
    with open(md_filename,"a+", encoding='utf-8') as f:

        if (use_title == True) and (to_web == True):
            f.write("---\n" + "layout: default\n" + "---\n\n")

        if show_badge == True:
            f.write(f"[![Contributors][contributors-shield]][contributors-url]\n")
            f.write(f"[![Forks][forks-shield]][forks-url]\n")
            f.write(f"[![Stargazers][stars-shield]][stars-url]\n")
            f.write(f"[![Issues][issues-shield]][issues-url]\n\n")

        if use_title == True:
            #f.write(("<p align="center"><h1 align="center"><br><ins>CV-ARXIV-DAILY"
            #         "</ins><br>Automatically Update CV Papers Daily</h1></p>\n"))
            f.write("## Updated on " + DateNow + "\n")
        else:
            f.write("> Updated on " + DateNow + "\n")

        # TODO: add usage
        f.write("> Usage instructions: [here](./docs/README.md#usage)\n\n")

        #Add: table of contents
        if use_tc == True:
            f.write("<details>\n")
            f.write("  <summary>Table of Contents</summary>\n")
            f.write("  <ol>\n")
            for keyword in data.keys():
                day_content = data[keyword]
                if not day_content:
                    continue
                kw = keyword.replace(' ','-')
                f.write(f"    <li><a href=#{kw.lower()}>{keyword}</a></li>\n")
            f.write("  </ol>\n")
            f.write("</details>\n\n")

        for keyword in data.keys():
            day_content = data[keyword]
            if not day_content:
                continue
            # the head of each part
            f.write(f"## {keyword}\n\n")

            if use_title == True :
                if to_web == False:
                    f.write("|Publish Date|Title|Authors|PDF|Code|\n" + "|---|---|---|---|---|\n")
                else:
                    f.write("| Publish Date | Title | Authors | PDF | Code |\n")
                    f.write("|:---------|:-----------------------|:---------|:------|:------|\n")

            # sort papers by date
            day_content = sort_papers(day_content)

            for _,v in day_content.items():
                if v is not None:
                    f.write(pretty_math(v)) # make latex pretty

            f.write(f"\n")

            #Add: back to top
            if use_b2t:
                top_info = f"#Updated on {DateNow}"
                top_info = top_info.replace(' ','-').replace('.','')
                f.write(f"<p align=right>(<a href={top_info.lower()}>back to top</a>)</p>\n\n")

        if show_badge == True:
            # we don't like long string, break it!
            f.write((f"[contributors-shield]: https://img.shields.io/github/"
                     f"contributors/Vincentqyw/cv-arxiv-daily.svg?style=for-the-badge\n"))
            f.write((f"[contributors-url]: https://github.com/Vincentqyw/"
                     f"cv-arxiv-daily/graphs/contributors\n"))
            f.write((f"[forks-shield]: https://img.shields.io/github/forks/Vincentqyw/"
                     f"cv-arxiv-daily.svg?style=for-the-badge\n"))
            f.write((f"[forks-url]: https://github.com/Vincentqyw/"
                     f"cv-arxiv-daily/network/members\n"))
            f.write((f"[stars-shield]: https://img.shields.io/github/stars/Vincentqyw/"
                     f"cv-arxiv-daily.svg?style=for-the-badge\n"))
            f.write((f"[stars-url]: https://github.com/Vincentqyw/"
                     f"cv-arxiv-daily/stargazers\n"))
            f.write((f"[issues-shield]: https://img.shields.io/github/issues/Vincentqyw/"
                     f"cv-arxiv-daily.svg?style=for-the-badge\n"))
            f.write((f"[issues-url]: https://github.com/Vincentqyw/"
                     f"cv-arxiv-daily/issues\n\n"))

    logging.info(f"{task} finished")

def demo(**config):
    # TODO: use config
    data_collector = []
    data_collector_web= []

    keywords = config['kv']
    max_results = config['max_results']
    publish_readme = config['publish_readme']
    publish_gitpage = config['publish_gitpage']
    publish_wechat = config['publish_wechat']
    show_badge = config['show_badge']

    b_update = config['update_paper_links']
    logging.info(f'Update Paper Link = {b_update}')
    if config['update_paper_links'] == False:
        logging.info(f"GET daily papers begin")
        for topic, keyword in keywords.items():
            logging.info(f"Keyword: {topic}")
            data, data_web = get_daily_papers(topic, query = keyword,
                                            max_results = max_results)
            data_collector.append(data)
            data_collector_web.append(data_web)
            print("\n")
        logging.info(f"GET daily papers end")

    # 1. update README.md file
    if publish_readme:
        json_file = config['json_readme_path']
        md_file   = config['md_readme_path']
        # update paper links
        if config['update_paper_links']:
            update_paper_links(json_file)
        else:
            # update json data
            update_json_file(json_file,data_collector)
        # json data to markdown
        json_to_md(json_file,md_file, task ='Update Readme', \
            show_badge = show_badge)

    # 2. update docs/index.md file (to gitpage)
    if publish_gitpage:
        json_file = config['json_gitpage_path']
        md_file   = config['md_gitpage_path']
        # TODO: duplicated update paper links!!!
        if config['update_paper_links']:
            update_paper_links(json_file)
        else:
            update_json_file(json_file,data_collector)
        json_to_md(json_file, md_file, task ='Update GitPage', \
            to_web = True, show_badge = show_badge, \
            use_tc=False, use_b2t=False)

    # 3. Update docs/wechat.md file
    if publish_wechat:
        json_file = config['json_wechat_path']
        md_file   = config['md_wechat_path']
        # TODO: duplicated update paper links!!!
        if config['update_paper_links']:
            update_paper_links(json_file)
        else:
            update_json_file(json_file, data_collector_web)
        json_to_md(json_file, md_file, task ='Update Wechat', \
            to_web=False, use_title= False, show_badge = show_badge)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--config_path',type=str, default='config.yaml',
                            help='configuration file path')
    parser.add_argument('--update_paper_links', default=False,
                        action="store_true",help='whether to update paper links etc.')
    args = parser.parse_args()
    config = load_config(args.config_path)
    config = {**config, 'update_paper_links':args.update_paper_links}
    demo(**config)
