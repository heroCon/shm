from conan import ConanFile
from conan.tools.files import copy


class ShmPubsubConan(ConanFile):
    name = "shm_pubsub"
    version = "0.4.0"
    license = "MIT"
    description = "Header-only crash-consistent shared-memory pub/sub for Linux"
    package_type = "header-library"
    exports_sources = "include/*", "LICENSE"
    no_copy_source = True

    def package(self):
        copy(self, "*", src=self.source_folder + "/include", dst=self.package_folder + "/include")
        copy(self, "LICENSE", src=self.source_folder, dst=self.package_folder + "/licenses")

    def package_info(self):
        self.cpp_info.bindirs = []
        self.cpp_info.libdirs = []
        self.cpp_info.system_libs = ["pthread", "rt"]
