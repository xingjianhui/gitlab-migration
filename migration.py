import os.path
import random
import time
import urllib.parse

import yaml
import requests
import gitlab
from concurrent.futures import ThreadPoolExecutor


def get_config():
    cf = 'config.yaml'
    with open(cf) as fd:
        return yaml.load(fd, yaml.SafeLoader)


class GitlabClient:

    def __init__(self, conf):
        self._config = conf
        self.cli = gitlab.Gitlab(url=conf['url'], private_token=conf['token'])

    def request(self, url, method, **kwargs):
        url = self._config['url'] + '/api/v4' + url
        headers = kwargs.get('headers', {})
        headers.update({'PRIVATE-TOKEN': self._config['token']})
        kwargs['headers'] = headers
        req = requests.request(method, url, **kwargs)
        return req

    def get_group_id(self, group):
        url = '/groups/{}'.format(urllib.parse.quote_plus(group))
        req = self.request(url, 'GET')
        return req.json().get('id')

    def get_group_file_name(self, group):
        return f'download_group_{group.replace("/", "_")}.tar.gz'

    def get_project_file_name(self, project):
        return f'download_project_{project.replace("/", "_")}.tar.gz'

    def export_group(self, group):
        id = self.get_group_id(group)
        if not id:
            return
        url = f'/groups/{id}/export'
        req = self.request(url, 'POST')
        if req.status_code != 202:
            return
        url = f'/groups/{urllib.parse.quote_plus(group)}/export/download'
        file_name = self.get_group_file_name(group)
        for _ in range(100):
            req = self.request(url, 'GET', stream=True)
            if req.status_code != 200:
                time.sleep(1)
            with open(file_name, 'wb') as fd:
                for chunk in req.iter_content(chunk_size=8192):
                    fd.write(chunk)
            req.close()
        return file_name

    def is_group_exited(self, group):
        path = '/groups/{}'.format(urllib.parse.quote_plus(group))
        req = self.request(path, 'GET')
        if req.status_code == 200:
            return True
        return

    def import_group(self, group, file_name):
        names = group.split('/')
        name = names[-1]
        with open(file_name, 'rb') as fd:
            parent_id = None
            if len(names) > 1:
                parent_group = '/'.join(names[:-1])
                parent_id = self.get_group_id(parent_group)
            req = self.cli.groups.import_group(fd, name=name, path=name, parent_id=parent_id)
            pass

    def is_project_exited(self, project):
        try:
            self.cli.projects.get(project)
        except gitlab.exceptions.GitlabGetError:
            return False
        return True

    def get_project(self, project):
        req = self.cli.projects.get(project)
        return req

    def archived_project(self, project):
        req = self.cli.projects.get(project)
        id = req.get_id()
        url = f'/projects/{id}/archive'
        self.request(url, 'POST')

    def get_import_project_status(self, project):
        req = self.get_project(project)
        return req.import_status

    def export_project(self, project):
        p = self.cli.projects.get(project)
        export = p.exports.create()

        export.refresh()
        while export.export_status != 'finished':
            time.sleep(1)
            export.refresh()

        # Download the result
        filename = self.get_project_file_name(project)
        with open(filename, 'wb') as f:
            export.download(streamed=True, action=f.write)
        if os.path.exists(filename):
            return filename

    def import_project(self, project, filename, path, name):
        st = time.time()
        namespace = None
        ns = project.split('/')
        if len(ns) > 1:
            namespace = '/'.join(ns[:-1])
        with open(filename, 'rb') as fd:
            output = self.cli.projects.import_project(fd, path=path, name=name, namespace=namespace)
        # Waiting for import complete
        status = self.get_project(project).import_status
        while status != 'finished':
            if status == 'failed':
                print(f'Failed import project {project}')
                return
            time.sleep(10)
            status = self.get_project(project).import_status
        et = time.time()
        print(f'Import {project} use {int(et-st)}s')


def migrate_group(group):
    dest = GitlabClient(gitlab_config['dest'])
    src = GitlabClient(gitlab_config['src'])
    if dest.is_group_exited(group['dest']):
        return f'Skipping! {group} is exited.'
    print(f'Start migrate group: {group}.')
    filename = src.export_group(group['src'])
    # filename = 'download_group_growing_infra.tar.gz'
    if not filename:
        return f'Not found file {filename}'
    dest.import_group(group['dest'], filename)
    for _ in range(100):
        print(f'Waiting migrate group: {group}.')
        if dest.is_group_exited(group['dest']):
            return f'Finish migrate group: {group}.'
        time.sleep(1)
    raise f'Import {group} failed!'


def migrate_project(project):
    dest = GitlabClient(gitlab_config['dest'])
    src = GitlabClient(gitlab_config['src'])
    src.archived_project(project['src'])
    if dest.is_project_exited(project['dest']):
        return f'Skipping! {project} is exited.'
    print(f'Start migrate project: {project}.')
    if not src.is_project_exited(project['src']):
        return f'Skipping! source {project} not exited.'
    filename = src.export_project(project['src'])
    # filename = 'download_group_growing_infra.tar.gz'
    if not filename:
        return f'Not found file {filename}'
    project_info = src.get_project(project['src'])
    dest.import_project(project['dest'], filename, name=project_info.name, path=project_info.path)
    for _ in range(100):
        # print(f'Waiting migrate project: {project}.')
        if dest.is_project_exited(project['dest']):
            return f'Finish migrate project: {project}.'
        time.sleep(10)
    return f'Import {project} failed!'


def main():
    _migrations = config.get('migrations', {})
    groups = _migrations.get('groups', [])
    projects = _migrations.get('projects', [])
    print('*'*10 + 'Start migrate groups' + '*'*10)
    with ThreadPoolExecutor(max_workers=5) as pool:
        results = pool.map(migrate_group, groups)
        for g in results:
            print(g)
    print('*'*10 + 'End migrate groups' + '*'*10)
    print('*'*10 + 'Start migrate projects' + '*'*10)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = pool.map(migrate_project, projects)
        for p in results:
            print(p)
    print('*'*10 + 'End migrate projects' + '*'*10)


if __name__ == '__main__':
    config = get_config()
    gitlab_config = config.get('gitlab', {})
    main()
