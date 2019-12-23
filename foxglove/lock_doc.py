"""

Copyright 2019 BBC. Licensed under the terms of the Apache License 2.0.

"""
from io import StringIO
import json
import os
import subprocess
import sys

import git

from foxglove.shared import LogLevel


class LockDoc:
    """
    Build a document that captures the parameters needed to repeat the current
    environmental conditions.
    """
    def __init__(self, target_model):
        """
        :param: target_model subclass of :class:`foxglove.Model`
        """
        self.logger = StringIO() # injected if needed
        self.target_model = target_model
    
    def log(self, msg, log_level=LogLevel.INFO):
        """
        log a string
        """
        # TODO levels and allow injection of a log handler
        if self.logger:
            msg = msg.strip()
            self.logger.write(msg+"\n")

    def get_code_references(self):
        """
        Get parameters needed to return code to this state at a later date.
        Currently only supports git.

        :returns: dict that is safe to serialise to JSON
        
        or raises NotImplemented or ValueError if not possible.
        """
        # TODO - this assumes TaBS module is executed as "python my_tabs.py"
        # also need "./my_tabs.py" and pipenv variants

        target_module = self.target_model.__class__.__module__
        executing_file = os.path.abspath(sys.modules[target_module].__file__)
        executing_file_path = os.path.abspath(os.path.dirname(executing_file))

        try:
            git_repo = git.Repo(executing_file_path, search_parent_directories=True)
        except (git.exc.InvalidGitRepositoryError, git.exc.NoSuchPathError):
            msg = "Not a git repo and only git is currently supported"
            raise NotImplementedError(msg)

        if git_repo.is_dirty():
            msg = "There are uncommitted changes so can't get committish"
            self.log(msg, LogLevel.WARNING)

        if git_repo.untracked_files:
            # log as warning but don't stop
            untracked = ", ".join(git_repo.untracked_files)
            self.log(f"There are untracked files: {untracked}", LogLevel.WARNING)

        # My understanding, which might be wrong, is that branch doesn't need to be recorded, just the commit-ish.
        # example of what is being done here...
        """
        mc-n357827:example_project parkes25$ git log
        commit 7f75cef7239ad8582187d7fbebddd4af3f410616 (HEAD -> master)
        Author: Si Parker <si.parker@bbc.co.uk>
        Date:   Tue Mar 19 13:15:13 2019 +0000
        
            hello world
        """
        # 7f75cef7239ad8582187d7fbebddd4af3f410616 is what we are getting here

        current_branch = git_repo.head.reference
        current_commit_ish = current_branch.commit.hexsha
        d = {'commit_ish': current_commit_ish}
        try:
            remote_origin = git_repo.remotes.origin.url
            d['origin_url'] = remote_origin
        except:
            pass

        # or give the local url. They could both be given but without uncommitted changes
        # there doesn't seem to be a reason in giving away local info.
        if 'origin_url' not in d:
            d['local_dir'] = git_repo.git_dir

        return {'git': d}

    def get_data_sources(self):
        """
        Examine self.tabs_module's data connections and find those that were
        resolved into connection parameters by using a catalogue lookup.

        :returns: dict that is safe to serialise to JSON. Key is the connection name.
        """
        d = {}
        for k, connector in self.target_model.datasets().items():

            if not connector.uses_dataset_discovery:
                continue

#  can foxglove eval on demand or is this needed?
#             # force dataset to load, this will ensure dataset discovery has evaluated
#             # connection parameters.
#             assert connection.data
            d[k] = connector.engine_params
        return d

    def get_code_dependencies(self):
        """
        Just pip freeze output for now.

        :returns: list in pip freeze format
        """
        pip_commands = ['pip', 'pip3', '/usr/local/bin/pip3']
        for pip_cmd in pip_commands:
            try:
                raw_stdout = subprocess.check_output([pip_cmd, 'freeze'])
            except FileNotFoundError:
                continue

            dependencies = raw_stdout.decode('ascii').split('\n')[0:-1]
            if dependencies:
                return dependencies
        else:
            msg = "Couldn't find pip executable in: {}"
            raise ValueError(msg.format(','.join(pip_commands)))

    def get_document(self):
        """
        Assemble the data and code parts. This method assumes sub documents don't make a namespace
        that overwrites anothers'.
        """
        d = {
            'code_local': self.get_code_references(),
            'data_connections': self.get_data_sources(),
            'code_dependencies': self.get_code_dependencies(),
             }

        # any additional info generated by the locking process
        self.logger.seek(0)
        logs = [l.strip() for l in self.logger.readlines()]
        if logs:
            d['lock_log'] = logs

        return d
    
    def relock(self, lock_doc):
        """
        Apply parameters from a previous build to self.tabs_module in order to re-create
        an old build.

        Throws an exception if not possible. Bit limiting, next step is to provide info
        needed at a system level to re-create the environment. e.g. package version
        numbers to apply.

        :param: lock_doc (str) in JSON format.
        :returns: boolean when self.tabs_module is at correct state.
        """
        lock_info = json.loads(lock_doc)

        if 'data_connections' in lock_info:
            for dataset_name, dataset_new_details in lock_info['data_connections'].items():
                dataset_connection = getattr(self.tabs_module, dataset_name)
                for k,v in dataset_new_details.items():
                    print(k,v)
                    setattr(dataset_connection, k, v)

        return True
