import requests
import tarfile
import tempfile
import json
import os
from os import listdir
from os.path import isfile, join, isdir
import uuid
import hashlib
import json
import gzip
import shutil
from requests.auth import HTTPBasicAuth
from . import ManifestCreator

class Registry:
    def __init__(self, registryPath, imagePath, stream = False, login=None, password=None, sslVerify=True):
        self.registryPath = registryPath
        self.imagePath = imagePath
        self.login = login
        self.password = password
        self.auth = None
        self.stream = stream
        self.sslVerify = sslVerify
        if(self.login):
            self.auth = HTTPBasicAuth(self.login, self.password)
    

    def get_manifest(self):
        return self.extract_from_tar_and_get_as_json(self.imagePath, "manifest.json")


    def get_config(self, name):
        return self.extract_from_tar_and_get_as_json(self.imagePath, name)


    def extract_from_tar_and_get_file(self, tarPath, fileToExtract):
        manifest = tarfile.open(tarPath)
        manifestStrFile = manifest.extractfile(fileToExtract)
        return manifestStrFile


    def read_and_parse_as_Utf8(self, toParse):
        manifestStr = (toParse.read()).decode("utf-8")
        toParse.close()
        return manifestStr


    def conditional_print(self, what, end=None):
        if(self.stream):
            if(end):
                print(what, end=end)
            else:
                print(what)


    def parse_as_json(self, toParse):
        return json.loads(toParse)


    def extract_from_tar_and_get_as_json(self, tarPath, fileToParse):
        loaded = self.extract_from_tar_and_get_file(tarPath, fileToParse)
        stringified = self.read_and_parse_as_Utf8(loaded)
        return self.parse_as_json(stringified)
    

    def extract_tar_file(self, tmpdirname):
        tar = tarfile.open(self.imagePath)
        tar.extractall(tmpdirname)
        tar.close()
        return True


    def process_image(self):
        manifestFile = self.get_manifest()[0]
        repoTags = manifestFile["RepoTags"]
        configLoc = manifestFile["Config"]

        with tempfile.TemporaryDirectory() as tmpdirname:
            for repo in repoTags:
                image, tag = self.get_image_tag(repo)
                self.conditional_print("[INFO] Extracting tar for " + image + " with tag: " + tag)
                self.extract_tar_file(tmpdirname)
                layers = manifestFile["Layers"]
                for layer in layers:
                    self.conditional_print("[INFO] Starting pushing layer " + layer)
                    status, url = self.start_pushing(image)
                    if(not status):
                        self.conditional_print("[ERROR] Something bad with starting upload")
                        return False
                    self.push_layer(os.path.join(tmpdirname, layer), image, url)
                self.conditional_print("[INFO] Pushing config")
                status, url = self.start_pushing(image)
                if(not status):
                    return False
                self.push_config(os.path.join(tmpdirname, configLoc), image, url)
                properlyFormattedLayers = []
                for layer in layers:
                    properlyFormattedLayers.append(os.path.join(tmpdirname, layer))
                creator = ManifestCreator(os.path.join(tmpdirname, configLoc), properlyFormattedLayers)
                registryManifest = creator.create_json()
                self.conditional_print("[INFO] Pushing manifest")
                self.push_manifest(registryManifest, image, tag)
                self.conditional_print("[INFO] Image pushed")
        return True
    

    def push_manifest(self, manifest, image, tag):
        headers = {"Content-Type": "application/vnd.docker.distribution.manifest.v2+json"}
        url = self.registryPath + "/v2/" + image + "/manifests/" + tag
        r = requests.put(url, headers=headers, data=manifest, auth = self.auth, verify = self.sslVerify)
        return r.status_code == 201


    def get_image_tag(self, processing):
        splitted = processing.split(":")
        image = splitted[0]
        tag = splitted[1]
        return image, tag


    def start_pushing(self, repository):
        self.conditional_print("[INFO] Upload started")
        r = requests.post(self.registryPath + "/v2/" + repository + "/blobs/uploads/", auth = self.auth, verify = self.sslVerify)
        uploadUrl = None
        if(r.headers.get("Location", None)):
            uploadUrl = r.headers.get("Location")
        return (r.status_code == 202), uploadUrl


    def push_layer(self, layerPath, repository, uploadUrl):
        self.chunked_upload(layerPath, uploadUrl)


    def push_config(self, layerPath, repository, uploadUrl):
        self.chunked_upload(layerPath, uploadUrl)


    def get_sha256_of_file(self, filepath):
        sha256hash = hashlib.sha256()
        with open(filepath, "rb") as f:
            while True:
                data = f.read(2097152)
                sha256hash.update(data)
                if not data:
                    break
        return sha256hash.hexdigest()


    def read_in_chunks(self, file_object, hashed, chunk_size=2097152):
        while True:
            data = file_object.read(chunk_size)
            hashed.update(data)
            if not data:
                break
            yield data

    def set_auth(self, authObj):
        self.atuh = authObj


    def chunked_upload(self, file, url):
        content_path = os.path.abspath(file)
        content_size = os.stat(content_path).st_size
        f = open(content_path, "rb")
        index = 0
        offset = 0
        headers = {}
        uploadUrl = url
        sha256hash = hashlib.sha256()
        
        for chunk in self.read_in_chunks(f, sha256hash):
            offset = index + len(chunk)
            headers['Content-Type'] = 'application/octet-stream'
            headers['Content-Length'] = str(len(chunk))
            headers['Content-Range'] = '%s-%s' % (index, offset)
            index = offset
            last = False
            if(offset == content_size):
                last = True
            try:
                self.conditional_print("Pushing... " + str(round((offset / content_size) * 100, 2)) + "%  ", end="\r" )
                if(last):
                    r = requests.put(uploadUrl + "&digest=sha256:" + str(sha256hash.hexdigest()), data=chunk, headers=headers, auth = self.auth, verify = self.sslVerify)
                else:
                    r = requests.patch(uploadUrl, data=chunk, headers=headers, auth = self.auth, verify = self.sslVerify)
                    if("Location" in r.headers):
                        uploadUrl = r.headers["Location"]

            except Exception as e:
                return False
        f.close()
        self.conditional_print("")

