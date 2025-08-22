'use client';

import Link from 'next/link';
import {
  ChartBarIcon,
  CogIcon,
  ArrowTrendingUpIcon,
  PlayIcon
} from '@heroicons/react/24/outline';

const strategies = [
  {
    id: 'mean_reversion',
    name: '횡보장 전략',
    description: '양방향 포지션으로 평균회귀를 노리는 전략',
    icon: CogIcon,
    href: '/mean-reversion',
    features: [
      '시나리오별 거래 시간 설정',
      'ATR 기반 변동성 필터링',
      '부분 축소를 통한 리스크 관리',
      '프리클로즈 시스템으로 안전성 확보'
    ]
  },
  {
    id: 'dca',
    name: '순환매매 전략',
    description: '물타기 방식의 단방향 포지션 전략',
    icon: ArrowTrendingUpIcon,
    href: '/dca',
    features: [
      '하락 시 단계별 물타기 진입',
      '목표 수익률 달성 시 자동 청산',
      '유지증거금률 기반 리스크 관리',
      '레버리지 조절 가능'
    ]
  }
];

export default function Home() {

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="text-center mb-12">
          <h1 className="text-4xl font-bold text-gray-900 mb-4">
            Trading Strategy Simulator
          </h1>
          <p className="text-xl text-gray-600 mb-2">
            BTC 1분봉 데이터를 활용한 전략 백테스팅 도구
          </p>
          <p className="text-gray-500">
            원하는 전략을 선택하여 시뮬레이션을 시작하세요
          </p>
        </div>

        {/* 전략 선택 카드 */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-8 max-w-4xl mx-auto">
          {strategies.map((strategy) => {
            const Icon = strategy.icon;
            return (
              <div key={strategy.id} className="bg-white rounded-lg shadow-md overflow-hidden hover:shadow-lg transition-shadow">
                <div className="p-6">
                  <div className="flex items-center mb-4">
                    <div className="p-3 bg-blue-50 rounded-lg mr-4">
                      <Icon className="h-8 w-8 text-blue-600" />
                    </div>
                    <div>
                      <h3 className="text-xl font-semibold text-gray-900">{strategy.name}</h3>
                      <p className="text-gray-600 text-sm">{strategy.description}</p>
                    </div>
                  </div>

                  <div className="mb-6">
                    <h4 className="text-sm font-medium text-gray-700 mb-3">주요 기능:</h4>
                    <ul className="space-y-2">
                      {strategy.features.map((feature, index) => (
                        <li key={index} className="flex items-center text-sm text-gray-600">
                          <div className="w-2 h-2 bg-blue-400 rounded-full mr-3 flex-shrink-0"></div>
                          {feature}
                        </li>
                      ))}
                    </ul>
                  </div>

                  <Link
                    href={strategy.href}
                    className="w-full bg-blue-600 text-white py-3 px-4 rounded-md hover:bg-blue-700 transition-colors flex items-center justify-center"
                  >
                    <PlayIcon className="h-5 w-5 mr-2" />
                    {strategy.name} 시작하기
                  </Link>
                </div>
              </div>
            );
          })}
        </div>

        {/* 사용 가능한 데이터 정보 */}
        <div className="mt-16 max-w-4xl mx-auto">
          <div className="bg-white rounded-lg shadow-md p-6">
            <div className="flex items-center mb-4">
              <ChartBarIcon className="h-6 w-6 text-green-600 mr-2" />
              <h2 className="text-xl font-semibold text-gray-900">사용 가능한 데이터</h2>
            </div>

            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              <div className="text-center p-4 bg-green-50 rounded-lg">
                <div className="text-2xl font-bold text-green-600">BTC/USDT</div>
                <div className="text-sm text-green-700">거래 페어</div>
              </div>

              <div className="text-center p-4 bg-blue-50 rounded-lg">
                <div className="text-2xl font-bold text-blue-600">1분봉</div>
                <div className="text-sm text-blue-700">캔들 간격</div>
              </div>

              <div className="text-center p-4 bg-purple-50 rounded-lg">
                <div className="text-2xl font-bold text-purple-600">8개월</div>
                <div className="text-sm text-purple-700">데이터 기간</div>
              </div>

              <div className="text-center p-4 bg-orange-50 rounded-lg">
                <div className="text-2xl font-bold text-orange-600">2024.08~2025.07</div>
                <div className="text-sm text-orange-700">분석 범위</div>
              </div>
            </div>

            <div className="mt-6 p-4 bg-gray-50 rounded-lg">
              <h3 className="text-sm font-medium text-gray-700 mb-2">💡 시뮬레이션 팁</h3>
              <ul className="text-sm text-gray-600 space-y-1">
                <li>• 각 전략은 서로 다른 시장 상황에 최적화되어 있습니다</li>
                <li>• 여러 월간 데이터를 비교하여 전략의 일관성을 확인하세요</li>
                <li>• 파라미터 조정을 통해 리스크-수익 비율을 최적화할 수 있습니다</li>
                <li>• 실제 거래 전에는 충분한 백테스팅을 권장합니다</li>
              </ul>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
