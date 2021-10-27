"""Contains functions for installing FFmpeg.

Follows the instructions here (https://trac.ffmpeg.org/wiki/CompilationGuide)
for compiling FFmpeg.
"""
from absl import flags

_BUILD_FFMPEG_FROM_SOURCE = flags.DEFINE_boolean(
    'build_ffmpeg_from_source', False, 'Whether to build ffmpeg from source')

FLAGS = flags.FLAGS

_APT_DEPS = [
    'autoconf', 'automake', 'build-essential', 'cmake', 'git-core',
    'libass-dev', 'libfreetype6-dev', 'libgnutls28-dev', 'libsdl2-dev',
    'libtool', 'libva-dev', 'libvdpau-dev', 'libvorbis-dev', 'libxcb1-dev',
    'libxcb-shm0-dev', 'libxcb-xfixes0-dev', 'meson', 'ninja-build',
    'pkg-config', 'texinfo', 'wget', 'yasm', 'zlib1g-dev', 'mercurial',
    'libnuma-dev bc'
]


def YumInstall(unused_vm):
  raise NotImplementedError()


def AptInstall(vm):
  """Installs FFmpeg on systems with the apt package manager."""
  if not _BUILD_FFMPEG_FROM_SOURCE.value:
    vm.InstallPackages('ffmpeg')
    return

  vm.InstallPackages(' '.join(_APT_DEPS))
  vm.Install('build_tools')

  vm.RemoteCommand('mkdir -p ~/ffmpeg_sources ~/bin')
  # Install NASM
  vm.RemoteCommand(
      'cd ~/ffmpeg_sources && wget https://www.nasm.us/pub/nasm/releasebuilds/'
      '2.15.03/nasm-2.15.03.tar.bz2 && tar xjvf nasm-2.15.03.tar.bz2 && '
      'cd nasm-2.15.03 && ./autogen.sh && PATH="$HOME/bin:$PATH" '
      './configure --prefix="$HOME/ffmpeg_build" --bindir="$HOME/bin" && '
      'make -j && make install'
      )
  # Install Yasm
  vm.RemoteCommand(
      'cd ~/ffmpeg_sources && wget -O yasm-1.3.0.tar.gz '
      'https://github.com/yasm/yasm/releases/download/v1.3.0/yasm-1.3.0.tar.gz && '
      'tar xzvf yasm-1.3.0.tar.gz && cd yasm-1.3.0 && ./configure '
      '--prefix="$HOME/ffmpeg_build" --bindir="$HOME/bin" && make -j && '
      'make install'
      )
  # Install libx264
  vm.RemoteCommand(
      'cd ~/ffmpeg_sources && git -C x264 pull 2> /dev/null || git clone '
      '--depth 1 https://code.videolan.org/videolan/x264 && cd x264 && '
      'PATH="$HOME/bin:$PATH" PKG_CONFIG_PATH="$HOME/ffmpeg_build/lib/'
      'pkgconfig" ./configure --prefix="$HOME/ffmpeg_build" '
      '--bindir="$HOME/bin" --enable-static --enable-pic && '
      'PATH="$HOME/bin:$PATH" make -j && make install'
      )
  # Install libx265
  vm.RemoteCommand(
      'cd ~/ffmpeg_sources && git clone https://github.com/videolan/x265 '
      '&& cd x265/build/linux && PATH="$HOME/bin:$PATH" cmake -G '
      '"Unix Makefiles" -DCMAKE_INSTALL_PREFIX="$HOME/ffmpeg_build" '
      '-DENABLE_SHARED=off ../../source && PATH="$HOME/bin:$PATH" make -j && '
      'make install'
      )
  # Install libvpx
  vm.RemoteCommand(
      'cd ~/ffmpeg_sources && git -C libvpx pull 2> /dev/null || git clone '
      '--depth 1 https://chromium.googlesource.com/webm/libvpx.git && '
      'cd libvpx && PATH="$HOME/bin:$PATH" ./configure '
      '--prefix="$HOME/ffmpeg_build" --disable-examples --disable-unit-tests '
      '--enable-vp9-highbitdepth --as=yasm && PATH="$HOME/bin:$PATH" make -j && '
      'make install'
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
  # Skip installation of AV1 libraries: libaom, libsvtav1, libdav1d

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
      '--enable-libass --enable-libfdk-aac '
      '--enable-libfreetype --enable-libmp3lame --enable-libopus '
      '--enable-libvorbis --enable-libvpx --enable-libx264 --enable-libx265 '
      '--enable-nonfree && PATH="$HOME/bin:$PATH" make -j && make install'
      )
