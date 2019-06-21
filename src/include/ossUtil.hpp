/*******************************************************************************
 Copyright (C) 2013 SequoiaDB Software Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU Affero General Public License, version 3,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program. If not, see <http://www.gnu.org/license/>.
*******************************************************************************/
#ifndef OSSUTIL_HPP__
#define OSSUTIL_HPP__

inline void ossSleepmicros(unsigned int s)
{
	struct timespec t;
	t.tv_sec = (time_t) (s/1000000);
	t.tv_nsec = 1000 * (s % 1000000);
	while(nanosleep(&t, &t) == -1 && errno==EINTR);
}

inline void ossSleepmilli(unsigned int s)
{
	ossSleepmicros( s * 1000);
}

typedef pid_t     OSSPID;
typedef pthread_t OSSTID;

inline OSSPID ossGetParentProcessID()
{
	return getppid();
}

inline OSSPID ossGetCurrentProcessID()
{
	return getpid();
}

inline OSSTID ossGetCurrentThreadID()
{
	return syscall(SYS_gettid);
}

#endif