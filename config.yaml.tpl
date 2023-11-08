gitlab:
  src:
    url: https://git.source.xxx
    token: xxx
  dest:
    url: http://git.destination.xxx
    token: xxx

migrations:
  groups:
    - src: group_name
      dest: group_name
    - src: group_name/grp1
      dest: group_name/grp1
  projects:
    - src: group_name/grp1/project1
      dest: group_name/grp1/project1
