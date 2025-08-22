'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { ChartBarIcon, CogIcon, ArrowTrendingUpIcon } from '@heroicons/react/24/outline';

const navigation = [
    { name: '홈', href: '/', icon: ChartBarIcon },
    { name: '횡보장 전략', href: '/mean-reversion', icon: CogIcon },
    { name: '순환매매 전략', href: '/dca', icon: ArrowTrendingUpIcon },
];

export default function Navigation() {
    const pathname = usePathname();

    return (
        <nav className="bg-white shadow-sm border-b">
            <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
                <div className="flex justify-between h-16">
                    <div className="flex">
                        <div className="flex-shrink-0 flex items-center">
                            <Link href="/" className="text-2xl font-bold text-blue-600">
                                Trading Simulator
                            </Link>
                        </div>
                        <div className="hidden sm:ml-6 sm:flex sm:space-x-8">
                            {navigation.map((item) => {
                                const Icon = item.icon;
                                const isActive = pathname === item.href;
                                return (
                                    <Link
                                        key={item.name}
                                        href={item.href}
                                        className={`${isActive
                                                ? 'border-blue-500 text-gray-900'
                                                : 'border-transparent text-gray-500 hover:border-gray-300 hover:text-gray-700'
                                            } inline-flex items-center px-1 pt-1 border-b-2 text-sm font-medium`}
                                    >
                                        <Icon className="h-4 w-4 mr-2" />
                                        {item.name}
                                    </Link>
                                );
                            })}
                        </div>
                    </div>
                </div>
            </div>
        </nav>
    );
}
