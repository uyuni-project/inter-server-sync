#
# spec file for package uyuni inter server sync
#
# Copyright (c) 2023 SUSE LLC
#
# All modifications and additions to the file contributed by third parties
# remain the property of their copyright owners, unless otherwise agreed
# upon. The license for this file, and modifications and additions to the
# file, is the same license as for the pristine package itself (unless the
# license for the pristine package is not an Open Source License, in which
# case the license is the MIT License). An "Open Source License" is a
# license that conforms to the Open Source Definition (Version 1.9)
# published by the Open Source Initiative.

# Please submit bugfixes or comments via https://bugs.opensuse.org/
#


%if 0%{?rhel} == 8
%global debug_package %{nil}
%endif
%if 0%{?rhel}
# Fix ERROR: No build ID note found in
%undefine _missing_build_ids_terminate_build
%endif

%global provider        github
%global provider_tld    com
%global org             uyuni-project
%global project         inter-server-sync
%global provider_prefix %{provider}.%{provider_tld}/%{org}/%{project}

Name:           %{project}
Version:        0.3.4
Release:        0
Summary:        Export/import data on a uyuni server
License:        Apache-2.0
Group:          System/Management
URL:            https://%{provider_prefix}
Source0:        %{name}-%{version}.tar.gz
Source1:        vendor.tar.gz
BuildRequires:  golang-packaging
%if 0%{?rhel}
BuildRequires:  golang >= 1.18
%else
BuildRequires:  golang(API) >= 1.20
%endif
BuildRequires:  rsyslog

Requires:       gzip
Requires:       logrotate
Requires:       rsyslog
Requires:       systemd

%description
Uyuni inter server sync tool
Used to export content from one server and import it in a target server.

%prep
%autosetup
tar -zxf %{SOURCE1}

%build
export GOFLAGS=-mod=vendor
%goprep %{provider_prefix}
%gobuild -ldflags "-X github.com/uyuni-project/inter-server-sync/cmd.Version=%{version}" ...

%install
%goinstall
%gosrc

%gofilelist

# Add config files for hub
install -d -m 0750 %{buildroot}%{_var}/log/hub

# Add syslog config to redirect logs to /var/log/hub/iss2.log
install -D -m 0644 release/hub-iss-syslogs.conf %{buildroot}%{_sysconfdir}/rsyslog.d/hub-iss.conf

#logrotate config
install -D -m 0644 release/hub-iss-logrotate.conf %{buildroot}%{_sysconfdir}/logrotate.d/inter-server-sync

%check
%if 0%{?rhel}
# Fix OBS debug_package execution.
rm -f %{buildroot}/usr/lib/debug/%{_bindir}/%{name}-%{version}-*.debug
%endif

%post
%if 0%{?rhel}
%systemd_postun rsyslog.service
%else
%service_del_postun rsyslog.service
%endif

%files -f file.lst

%defattr(-,root,root)
%doc README.md
%license LICENSES
%{_bindir}/inter-server-sync

%config(noreplace) %{_sysconfdir}/rsyslog.d/hub-iss.conf
%config(noreplace) %{_sysconfdir}/logrotate.d/inter-server-sync

%changelog
