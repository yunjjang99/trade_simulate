'use client';

import { ArrowPathIcon } from '@heroicons/react/24/outline';

interface LoadingModalProps {
    isVisible: boolean;
    progress: number;
}

export default function LoadingModal({ isVisible, progress }: LoadingModalProps) {
    if (!isVisible) return null;

    return (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
            <div className="bg-white rounded-lg p-8 max-w-md w-full mx-4">
                <div className="text-center">
                    <div className="mb-4">
                        <ArrowPathIcon className="h-12 w-12 text-blue-600 mx-auto animate-spin" />
                    </div>
                    <h3 className="text-lg font-semibold text-gray-900 mb-2">
                        시뮬레이션 실행 중...
                    </h3>
                    <p className="text-gray-600 mb-4">
                        데이터를 분석하고 있습니다. 잠시만 기다려주세요.
                    </p>

                    {/* 진행률 바 */}
                    <div className="w-full bg-gray-200 rounded-full h-2.5 mb-4">
                        <div
                            className="bg-blue-600 h-2.5 rounded-full transition-all duration-300 ease-out"
                            style={{ width: `${progress}%` }}
                        ></div>
                    </div>

                    <div className="flex justify-between text-sm text-gray-500">
                        <span>진행률</span>
                        <span>{progress}%</span>
                    </div>
                </div>
            </div>
        </div>
    );
}
