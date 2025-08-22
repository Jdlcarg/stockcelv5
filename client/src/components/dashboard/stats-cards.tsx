import { Card, CardContent } from "@/components/ui/card";
import { Smartphone, DollarSign, Clock, AlertTriangle } from "lucide-react";

interface StatsCardsProps {
  stats?: {
    totalProducts: number;
    monthlySales: number;
    pendingOrders: number;
    lowStock: number;
  };
}

export default function StatsCards({ stats }: StatsCardsProps) {
  if (!stats) {
    return (
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {[1, 2, 3, 4].map((i) => (
          <Card key={i} className="animate-pulse">
            <CardContent className="p-6">
              <div className="h-20 bg-gray-200 rounded"></div>
            </CardContent>
          </Card>
        ))}
      </div>
    );
  }

  const cards = [
    {
      title: "Total Productos",
      value: stats.totalProducts.toLocaleString(),
      icon: Smartphone,
      iconBg: "bg-primary/10",
      iconColor: "text-primary",
    },
    {
      title: "Ventas del Mes",
      value: `$${stats.monthlySales.toLocaleString()}`,
      icon: DollarSign,
      iconBg: "bg-accent/10",
      iconColor: "text-accent",
    },
    {
      title: "Pedidos Pendientes",
      value: stats.pendingOrders.toString(),
      icon: Clock,
      iconBg: "bg-warning/10",
      iconColor: "text-warning",
    },
    {
      title: "Stock Bajo",
      value: stats.lowStock.toString(),
      icon: AlertTriangle,
      iconBg: "bg-red-100",
      iconColor: "text-red-600",
    },
  ];

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
      {cards.map((card, index) => {
        const Icon = card.icon;
        return (
          <Card key={index} className="border border-gray-200 dark:border-gray-700">
            <CardContent className="p-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium text-gray-600 dark:text-gray-400">{card.title}</p>
                  <p className="text-2xl font-bold text-gray-900 dark:text-white">{card.value}</p>
                </div>
                <div className={`h-12 w-12 ${card.iconBg} rounded-lg flex items-center justify-center`}>
                  <Icon className={`h-6 w-6 ${card.iconColor}`} />
                </div>
              </div>
            </CardContent>
          </Card>
        );
      })}
    </div>
  );
}
