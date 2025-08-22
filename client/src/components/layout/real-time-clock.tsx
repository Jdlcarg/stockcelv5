import { useState, useEffect } from 'react';
import { Clock } from 'lucide-react';

interface RealTimeClockProps {
  className?: string;
  showDate?: boolean;
  format24h?: boolean;
}

export default function RealTimeClock({
  className = "",
  showDate = false,
  format24h = true
}: RealTimeClockProps) {
  const [currentTime, setCurrentTime] = useState('');
  const [currentDate, setCurrentDate] = useState('');

  useEffect(() => {
    const updateTime = () => {
      const now = new Date();

      // Get Argentina time
      const argentinaTime = new Intl.DateTimeFormat('es-AR', {
        timeZone: 'America/Argentina/Buenos_Aires',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false,
      }).format(now);

      const argentinaDate = new Intl.DateTimeFormat('es-AR', {
        timeZone: 'America/Argentina/Buenos_Aires',
        weekday: 'long',
        year: 'numeric',
        month: 'long',
        day: 'numeric',
      }).format(now);

      setCurrentTime(argentinaTime);
      setCurrentDate(argentinaDate.charAt(0).toUpperCase() + argentinaDate.slice(1)); // Capitalize first letter
    };

    updateTime();
    const interval = setInterval(updateTime, 1000);

    return () => clearInterval(interval);
  }, []);

  return (
    <div className={`flex items-center space-x-2 ${className}`}>
      <Clock className="h-4 w-4 text-gray-500" />
      <div className="text-right">
        <div className="font-mono text-sm font-medium text-gray-900 dark:text-gray-100">
          {currentTime}
        </div>
        {showDate && (
          <div className="text-xs text-gray-600 dark:text-gray-400">
            {currentDate}
          </div>
        )}
      </div>
    </div>
  );
}