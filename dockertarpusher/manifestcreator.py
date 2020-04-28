import os
import hashlib
import json

class ManifestCreator():
    def __init__(self, configPath, layersPaths):
        self.configPath = configPath
        self.layersPaths = layersPaths
    

    def create_json(self):
        resultDict = dict()
        resultDict["schemaVersion"] = 2
        resultDict["mediaType"] = "application/vnd.docker.distribution.manifest.v2+json"
        resultDict["config"] = dict()
        resultDict["config"]["mediaType"] = "application/vnd.docker.container.image.v1+json"

        resultDict["config"]["size"] = self.getSizeOf(self.configPath)
        resultDict["config"]["digest"] = self.get_sha256_properly_formatted(self.configPath)

        resultDict["layers"] = []
        for layer in self.layersPaths:
            layerDict = dict()
            layerDict["mediaType"] = "application/vnd.docker.image.rootfs.diff.tar"
            layerDict["size"] = self.getSizeOf(layer)
            layerDict["digest"] = self.get_sha256_properly_formatted(layer)
            resultDict["layers"].append(layerDict)

        return json.dumps(resultDict)


    def getSizeOf(self, path):
        return os.path.getsize(path)


    def get_sha256_of_file(self, filepath):
        sha256hash = hashlib.sha256()
        with open(filepath, "rb") as f:
            while True:
                data = f.read(65536)
                sha256hash.update(data)
                if not data:
                    break
        return sha256hash.hexdigest()
        

    def get_sha256_properly_formatted(self, filepath):
        return "sha256:" + self.get_sha256_of_file(filepath)
