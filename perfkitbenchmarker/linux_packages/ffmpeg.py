"""Contains functions for installing FFmpeg.

Follows the instructions here (https://trac.ffmpeg.org/wiki/CompilationGuide)
for compiling FFmpeg.
Follows the instructions here
(https://docs.nvidia.com/video-technologies/video-codec-sdk/12.0/ffmpeg-with-nvidia-gpu/index.html)
for compiling FFmpeg on Nvidia GPU's.
"""

from absl import flags

_BUILD_FFMPEG_FROM_SOURCE = flags.DEFINE_boolean(
    'build_ffmpeg_from_source', True, 'Whether to build ffmpeg from source'
)

FLAGS = flags.FLAGS

_APT_DEPS = [
    'autoconf',
    'automake',
    'build-essential',
    'cmake',
    'git-core',
    'libass-dev',
    'libfreetype6-dev',
    'libgnutls28-dev',
    'libsdl2-dev',
    'libtool',
    'libva-dev',
    'libvdpau-dev',
    'libvorbis-dev',
    'libxcb1-dev',
    'libxcb-shm0-dev',
    'libxcb-xfixes0-dev',
    'meson',
    'ninja-build',
    'pkg-config',
    'texinfo',
    'wget',
    'yasm',
    'zlib1g-dev',
    'mercurial',
    'libnuma-dev bc',
]

_NV_CODEC_TAG = 'n12.1.14.0'
_NV_CODEC_REPO = 'https://git.videolan.org/git/ffmpeg/nv-codec-headers.git'
_LIBX264_TAG = 'b35605ace3ddf7c1a5d67a2eb553f034aef41d55'
_LIBX265_TAG = '4.1'
_LIBAOM_TAG = 'v3.12.1'


def YumInstall(unused_vm):
  raise NotImplementedError()


def AptInstall(vm):
  """Installs FFmpeg on systems with the apt package manager."""
  if not _BUILD_FFMPEG_FROM_SOURCE.value:
    vm.InstallPackages('ffmpeg')
    return

  stdout, _ = vm.RemoteCommand(
      'if lspci | grep -i "NVIDIA Corporation" >/dev/null; then echo "Contains'
      ' GPU"; else echo "Does not contain GPU"; fi'
  )
  if 'Contains GPU' in stdout:
    vm.Install('cuda_toolkit')
    vm.InstallPackages('nvidia-cuda-toolkit')
    vm.InstallPackages(
        'build-essential yasm cmake libtool libc6 libc6-dev unzip wget libnuma1'
        ' libnuma-dev libx264-dev libx265-dev libunistring-dev libaom-dev'
        ' libdav1d-dev pkg-config nasm'
    )
    vm.RemoteCommand(f'git clone --branch {_NV_CODEC_TAG} {_NV_CODEC_REPO}')
    vm.RemoteCommand('cd nv-codec-headers && sudo make install')
    vm.RemoteCommand('git clone https://git.ffmpeg.org/ffmpeg.git ffmpeg/')
    vm.RemoteCommand(
        'cd ffmpeg && ./configure --enable-nonfree --enable-cuda-nvcc'
        ' --nvccflags="-gencode arch=compute_52,code=sm_52 -O2" --enable-libnpp'
        ' --extra-cflags=-I/usr/local/cuda/include'
        ' --extra-ldflags=-L/usr/local/cuda/lib64 --disable-static'
        ' --enable-shared --enable-gpl --enable-libx264 --enable-libx265'
    )
    vm.RemoteCommand('cd ffmpeg && make -j 8')
    vm.RemoteCommand('cd ffmpeg && sudo make install')
    vm.RemoteCommand('echo "usr/local/lib" | sudo tee -a /etc/ld.so.conf')
    vm.RemoteCommand('sudo ldconfig')
  else:
    vm.InstallPackages(' '.join(_APT_DEPS))
    vm.Install('build_tools')

    vm.RemoteCommand('mkdir -p ~/ffmpeg_sources ~/bin')
    # Install NASM
    vm.RemoteCommand(
        'cd ~/ffmpeg_sources && wget https://www.nasm.us/pub/nasm/releasebuilds/'
        '2.16.03/nasm-2.16.03.tar.bz2 && tar xjvf nasm-2.16.03.tar.bz2 && '
        'cd nasm-2.16.03 && ./autogen.sh && PATH="$HOME/bin:$PATH" '
        './configure --prefix="$HOME/ffmpeg_build" --bindir="$HOME/bin" && '
        'make -j && make install'
    )
    # Install Yasm
    vm.RemoteCommand(
        'cd ~/ffmpeg_sources && wget -O yasm-1.3.0.tar.gz'
        ' https://github.com/yasm/yasm/releases/download/v1.3.0/yasm-1.3.0.tar.gz'
        ' && tar xzvf yasm-1.3.0.tar.gz && cd yasm-1.3.0 && ./configure'
        ' --prefix="$HOME/ffmpeg_build" --bindir="$HOME/bin" && make -j && make'
        ' install'
    )
    # Install libx264
    vm.RemoteCommand(
        'cd ~/ffmpeg_sources && git -C x264 pull 2> /dev/null || git clone '
        'https://code.videolan.org/videolan/x264 && cd x264 && '
        f'git checkout {_LIBX264_TAG} && PATH="$HOME/bin:$PATH" '
        'PKG_CONFIG_PATH="$HOME/ffmpeg_build/lib/ pkgconfig" ./configure '
        '--prefix="$HOME/ffmpeg_build" --bindir="$HOME/bin" --enable-static '
        '--enable-pic && PATH="$HOME/bin:$PATH" make -j && make install'
    )
    # Install libx265
    vm.RemoteCommand(
        'sudo apt-get install libnuma-dev && cd ~/ffmpeg_sources && git clone '
        'https://bitbucket.org/multicoreware/x265_git.git && cd x265_git && '
        f'git checkout {_LIBX265_TAG} && cd build/linux && '
        'PATH="$HOME/bin:$PATH" cmake -G "Unix Makefiles" '
        '-DCMAKE_INSTALL_PREFIX="$HOME/ffmpeg_build" -DENABLE_SHARED=off '
        '../../source && PATH="$HOME/bin:$PATH" make && make install'
    )
    # Install libvpx
    vm.RemoteCommand(
        'cd ~/ffmpeg_sources && git -C libvpx pull 2> /dev/null || git clone'
        ' --depth 1 https://chromium.googlesource.com/webm/libvpx.git && cd'
        ' libvpx && PATH="$HOME/bin:$PATH" ./configure'
        ' --prefix="$HOME/ffmpeg_build" --disable-examples --disable-unit-tests'
        ' --enable-vp9-highbitdepth --as=yasm && PATH="$HOME/bin:$PATH" make -j'
        ' && make install'
    )
    # Install libfdk-aac
    vm.RemoteCommand(
        'cd ~/ffmpeg_sources && git -C fdk-aac pull 2> /dev/null || git clone '
        '--depth 1 https://github.com/mstorsjo/fdk-aac && cd fdk-aac && '
        'autoreconf -fiv && ./configure --prefix="$HOME/ffmpeg_build" '
        '--disable-shared && make -j && make install'
    )
    # Install libmp3lame
    vm.RemoteCommand(
        'cd ~/ffmpeg_sources && wget -O lame-3.100.tar.gz '
        'https://downloads.sourceforge.net/project/lame/lame/3.100/'
        'lame-3.100.tar.gz && tar xzvf lame-3.100.tar.gz && cd lame-3.100 && '
        'PATH="$HOME/bin:$PATH" ./configure --prefix="$HOME/ffmpeg_build" '
        '--bindir="$HOME/bin" --disable-shared --enable-nasm && '
        'PATH="$HOME/bin:$PATH" make -j && make install'
    )
    # Install libopus
    vm.RemoteCommand(
        'cd ~/ffmpeg_sources && git -C opus pull 2> /dev/null || git clone '
        '--depth 1 https://github.com/xiph/opus.git && cd opus && '
        './autogen.sh && ./configure --prefix="$HOME/ffmpeg_build" '
        '--disable-shared && make -j && make install'
    )
    # Install libaom
    vm.RemoteCommand(
        'cd ~/ffmpeg_sources && git -C aom pull 2> /dev/null || git clone '
        'https://aomedia.googlesource.com/aom && cd aom && git '
        f'checkout {_LIBAOM_TAG} && cd .. && mkdir -p aom_build && cd '
        'aom_build && PATH="$HOME/bin:$PATH" cmake -G "Unix Makefiles" '
        '-DCMAKE_INSTALL_PREFIX="$HOME/ffmpeg_build" -DENABLE_TESTS=OFF '
        '-DENABLE_NASM=on ../aom && PATH="$HOME/bin:$PATH" make && make '
        'install'
    )
    # Install libsvtav1
    vm.RemoteCommand(
        'cd ~/ffmpeg_sources && git -C SVT-AV1 pull 2> /dev/null || git clone'
        ' https://gitlab.com/AOMediaCodec/SVT-AV1.git && mkdir -p SVT-AV1/build'
        ' && cd SVT-AV1/build && PATH="$HOME/bin:$PATH" cmake -G "Unix'
        ' Makefiles" -DCMAKE_INSTALL_PREFIX="$HOME/ffmpeg_build"'
        ' -DCMAKE_BUILD_TYPE=Release -DBUILD_DEC=OFF -DBUILD_SHARED_LIBS=OFF ..'
        ' && PATH="$HOME/bin:$PATH" make && make install'
    )
    # Install libdav1d
    vm.RemoteCommand(
        'cd ~/ffmpeg_sources && git -C dav1d pull 2> /dev/null || git clone '
        '--depth 1 https://code.videolan.org/videolan/dav1d.git && mkdir -p '
        'dav1d/build && cd dav1d/build && meson setup -Denable_tools=false '
        '-Denable_tests=false --default-library=static .. --prefix '
        '"$HOME/ffmpeg_build" --libdir="$HOME/ffmpeg_build/lib" && ninja && '
        'ninja install'
    )
    # Install FFmpeg
    vm.RemoteCommand(
        'cd ~/ffmpeg_sources && wget -O ffmpeg-snapshot.tar.bz2 '
        'https://ffmpeg.org/releases/ffmpeg-snapshot.tar.bz2 && '
        'tar xjvf ffmpeg-snapshot.tar.bz2 && cd ffmpeg && '
        'PATH="$HOME/bin:$PATH" PKG_CONFIG_PATH="$HOME/ffmpeg_build/lib/'
        'pkgconfig" ./configure --prefix="$HOME/ffmpeg_build" '
        '--pkg-config-flags="--static" --extra-cflags="-I$HOME/ffmpeg_build/'
        'include" --extra-ldflags="-L$HOME/ffmpeg_build/lib" '
        '--extra-libs="-lpthread -lm" --bindir="$HOME/bin" --enable-gpl '
        '--enable-libaom --enable-libass --enable-libfdk-aac '
        '--enable-libfreetype --enable-libmp3lame --enable-libopus '
        '--enable-libsvtav1 --enable-libdav1d --enable-libvorbis '
        '--enable-libvpx --enable-libx264 --enable-libx265 --enable-nonfree && '
        'PATH="$HOME/bin:$PATH" make -j && make install'
    )
