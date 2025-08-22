'use client';

const AVAILABLE_FILES = [
    { filename: 'BTCUSDT-1m-2024-08.csv', month: '2024-08', name: '2024년 8월' },
    { filename: 'BTCUSDT-1m-2025-01.csv', month: '2025-01', name: '2025년 1월' },
    { filename: 'BTCUSDT-1m-2025-02.csv', month: '2025-02', name: '2025년 2월' },
    { filename: 'BTCUSDT-1m-2025-03.csv', month: '2025-03', name: '2025년 3월' },
    { filename: 'BTCUSDT-1m-2025-04.csv', month: '2025-04', name: '2025년 4월' },
    { filename: 'BTCUSDT-1m-2025-05.csv', month: '2025-05', name: '2025년 5월' },
    { filename: 'BTCUSDT-1m-2025-06.csv', month: '2025-06', name: '2025년 6월' },
    { filename: 'BTCUSDT-1m-2025-07.csv', month: '2025-07', name: '2025년 7월' },
];

interface DataFileSelectorProps {
    selectedFile: string;
    onFileChange: (filename: string) => void;
}

export default function DataFileSelector({ selectedFile, onFileChange }: DataFileSelectorProps) {
    return (
        <div className="mb-6">
            <label className="block text-sm font-medium text-gray-700 mb-2">
                데이터 파일
            </label>
            <select
                value={selectedFile}
                onChange={(e) => onFileChange(e.target.value)}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
                <option value="">파일을 선택하세요</option>
                {AVAILABLE_FILES.map((file) => (
                    <option key={file.filename} value={file.filename}>
                        {file.name} ({file.filename})
                    </option>
                ))}
            </select>
        </div>
    );
}
