Name: bob
Summary: Bob is distributing storage system
License: MIT
Version: current_version
Release: release_number
Source0: %{name}-%{version}.tar.gz
Group: Applications/Databases
BuildArch: x86_64

%global debug_package %{nil}
%define _binaries_in_noarch_packages_terminate_build 0

%description
Bob is distributing storage system designed for byte data like photos. It is has decentralized architecture where each node can handleuser calls. Pearl uses like backend storage.

%prep
%setup -q

%build

%install
rm -rf %{buildroot}
mkdir -p %{buildroot}/usr/bin/
mkdir -p %{buildroot}/etc/bob/
mkdir -p %{buildroot}/etc/systemd/system/
mkdir -p %{buildroot}/lib/systemd/system/
mkdir -p %{buildroot}/etc/security/limits.d/
cp %{_builddir}/%{name}-%{version}/bobd %{buildroot}/usr/bin/
cp %{_builddir}/%{name}-%{version}/bobp %{buildroot}/usr/bin/
cp %{_builddir}/%{name}-%{version}/ccg %{buildroot}/usr/bin/
cp %{_builddir}/%{name}-%{version}/config-examples/cluster.yaml %{buildroot}/etc/bob/
cp %{_builddir}/%{name}-%{version}/config-examples/node.yaml %{buildroot}/etc/bob/
cp %{_builddir}/%{name}-%{version}/config-examples/logger.yaml %{buildroot}/etc/bob/
cp bob.service %{buildroot}/etc/systemd/system/
cp bob.service %{buildroot}/lib/systemd/system/
cp bob_limits.conf %{buildroot}/etc/security/limits.d/bob.conf

%clean
rm -rf %{buildroot}

%post
# Add user
BOB_USER=bob
if ! getent passwd ${BOB_USER} > /dev/null; then
  adduser --system ${BOB_USER} --create-home > /dev/null
fi

%files
/usr/bin/bobd
/usr/bin/bobp
/usr/bin/ccg
%config(noreplace) /etc/bob/cluster.yaml
%config(noreplace) /etc/bob/node.yaml
%config(noreplace) /etc/bob/logger.yaml
/etc/systemd/system/bob.service
/lib/systemd/system/bob.service
/etc/security/limits.d/bob.conf