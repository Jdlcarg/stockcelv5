import { useState, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import Header from "@/components/layout/header";
import Sidebar from "@/components/layout/sidebar";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { 
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { 
  Trophy, 
  TrendingUp, 
  DollarSign, 
  Users, 
  ShoppingCart,
  Calendar,
  Filter,
  Download
} from "lucide-react";

export default function VendorPerformance() {
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo, setDateTo] = useState("");
  const [selectedPeriod, setSelectedPeriod] = useState("month");
  const [selectedVendor, setSelectedVendor] = useState<string>("");

  // Set default date range (last 30 days)
  useEffect(() => {
    const today = new Date();
    const thirtyDaysAgo = new Date(today.getTime() - 30 * 24 * 60 * 60 * 1000);
    
    setDateFrom(thirtyDaysAgo.toISOString().split('T')[0]);
    setDateTo(today.toISOString().split('T')[0]);
  }, []);

  // Get vendor ranking
  const { data: vendorRanking, isLoading: isLoadingRanking, refetch: refetchRanking } = useQuery({
    queryKey: ['/api/vendor-performance/ranking', { dateFrom, dateTo }],
    queryFn: async () => {
      const params = new URLSearchParams({
        clientId: '1',
        ...(dateFrom && { dateFrom }),
        ...(dateTo && { dateTo })
      });
      const response = await fetch(`/api/vendor-performance/ranking?${params}`);
      if (!response.ok) throw new Error('Failed to fetch vendor ranking');
      return response.json();
    },
    enabled: !!dateFrom && !!dateTo
  });

  // Get vendor profits
  const { data: vendorProfits, isLoading: isLoadingProfits, refetch: refetchProfits } = useQuery({
    queryKey: ['/api/vendor-performance/profits', { dateFrom, dateTo, selectedVendor }],
    queryFn: async () => {
      const params = new URLSearchParams({
        clientId: '1',
        ...(dateFrom && { dateFrom }),
        ...(dateTo && { dateTo }),
        ...(selectedVendor && { vendorId: selectedVendor })
      });
      const response = await fetch(`/api/vendor-performance/profits?${params}`);
      if (!response.ok) throw new Error('Failed to fetch vendor profits');
      return response.json();
    },
    enabled: !!dateFrom && !!dateTo
  });

  // Get performance summary
  const { data: performanceSummary, isLoading: isLoadingSummary } = useQuery({
    queryKey: ['/api/vendor-performance/summary', { period: selectedPeriod }],
    queryFn: async () => {
      const params = new URLSearchParams({
        clientId: '1',
        period: selectedPeriod
      });
      const response = await fetch(`/api/vendor-performance/summary?${params}`);
      if (!response.ok) throw new Error('Failed to fetch performance summary');
      return response.json();
    }
  });

  const handleApplyFilters = () => {
    refetchRanking();
    refetchProfits();
  };

  const getRankBadgeColor = (rank: number) => {
    switch (rank) {
      case 1: return "bg-yellow-500 hover:bg-yellow-600"; // Gold
      case 2: return "bg-gray-400 hover:bg-gray-500"; // Silver
      case 3: return "bg-amber-600 hover:bg-amber-700"; // Bronze
      default: return "bg-blue-500 hover:bg-blue-600"; // Default
    }
  };

  const getRankIcon = (rank: number) => {
    if (rank <= 3) {
      return <Trophy className="h-4 w-4" />;
    }
    return null;
  };

  if (isLoadingRanking || isLoadingProfits || isLoadingSummary) {
    return (
      <div className="flex h-screen bg-gray-50 dark:bg-gray-900">
        <Sidebar />
        <div className="flex-1 flex flex-col">
          <Header />
          <div className="flex-1 flex items-center justify-center">
            <div className="text-lg">Cargando reportes de vendedores...</div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="flex h-screen bg-gray-50 dark:bg-gray-900">
      <Sidebar />
      <div className="flex-1 flex flex-col overflow-hidden">
        <Header onMobileMenuToggle={() => {}} />
        
        <div className="flex-1 overflow-auto p-6">
          <div className="max-w-7xl mx-auto space-y-6">
            {/* Header Section */}
            <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
              <div>
                <h1 className="text-3xl font-bold text-gray-900 dark:text-white">
                  Rendimiento de Vendedores
                </h1>
                <p className="text-gray-600 dark:text-gray-400 mt-1">
                  Análisis de ganancias y ranking por vendedor
                </p>
              </div>
              
              <div className="flex items-center gap-2">
                <Button variant="outline" size="sm">
                  <Download className="h-4 w-4 mr-2" />
                  Exportar
                </Button>
              </div>
            </div>

            {/* Performance Summary Cards */}
            {performanceSummary && (
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                <Card>
                  <CardContent className="p-6">
                    <div className="flex items-center justify-between">
                      <div>
                        <p className="text-sm font-medium text-gray-600 dark:text-gray-400">
                          Ingresos Totales
                        </p>
                        <p className="text-2xl font-bold text-green-600">
                          ${performanceSummary.totalRevenue}
                        </p>
                      </div>
                      <DollarSign className="h-8 w-8 text-green-600" />
                    </div>
                  </CardContent>
                </Card>

                <Card>
                  <CardContent className="p-6">
                    <div className="flex items-center justify-between">
                      <div>
                        <p className="text-sm font-medium text-gray-600 dark:text-gray-400">
                          Total Órdenes
                        </p>
                        <p className="text-2xl font-bold text-blue-600">
                          {performanceSummary.totalOrders}
                        </p>
                      </div>
                      <ShoppingCart className="h-8 w-8 text-blue-600" />
                    </div>
                  </CardContent>
                </Card>

                <Card>
                  <CardContent className="p-6">
                    <div className="flex items-center justify-between">
                      <div>
                        <p className="text-sm font-medium text-gray-600 dark:text-gray-400">
                          Vendedores Activos
                        </p>
                        <p className="text-2xl font-bold text-purple-600">
                          {performanceSummary.activeVendors}/{performanceSummary.totalVendors}
                        </p>
                      </div>
                      <Users className="h-8 w-8 text-purple-600" />
                    </div>
                  </CardContent>
                </Card>

                <Card>
                  <CardContent className="p-6">
                    <div className="flex items-center justify-between">
                      <div>
                        <p className="text-sm font-medium text-gray-600 dark:text-gray-400">
                          Mejor Vendedor
                        </p>
                        <p className="text-lg font-bold text-yellow-600">
                          {performanceSummary.topPerformer.vendorName}
                        </p>
                        <p className="text-sm text-gray-500">
                          ${performanceSummary.topPerformer.sales}
                        </p>
                      </div>
                      <Trophy className="h-8 w-8 text-yellow-600" />
                    </div>
                  </CardContent>
                </Card>
              </div>
            )}

            {/* Filters Section */}
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Filter className="h-5 w-5" />
                  Filtros de Período
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                  <div>
                    <label className="text-sm font-medium mb-2 block">Fecha Desde</label>
                    <Input
                      type="date"
                      value={dateFrom}
                      onChange={(e) => setDateFrom(e.target.value)}
                    />
                  </div>
                  <div>
                    <label className="text-sm font-medium mb-2 block">Fecha Hasta</label>
                    <Input
                      type="date"
                      value={dateTo}
                      onChange={(e) => setDateTo(e.target.value)}
                    />
                  </div>
                  <div>
                    <label className="text-sm font-medium mb-2 block">Vendedor</label>
                    <Select value={selectedVendor} onValueChange={setSelectedVendor}>
                      <SelectTrigger>
                        <SelectValue placeholder="Todos los vendedores" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="">Todos los vendedores</SelectItem>
                        {vendorRanking?.map((vendor: any) => (
                          <SelectItem key={vendor.vendorId} value={vendor.vendorId.toString()}>
                            {vendor.vendorName}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="flex items-end">
                    <Button onClick={handleApplyFilters} className="w-full">
                      Aplicar Filtros
                    </Button>
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Main Content Tabs */}
            <Tabs defaultValue="ranking" className="space-y-4">
              <TabsList className="grid w-full grid-cols-2">
                <TabsTrigger value="ranking">Ranking de Vendedores</TabsTrigger>
                <TabsTrigger value="profits">Reporte de Ganancias</TabsTrigger>
              </TabsList>

              {/* Vendor Ranking Tab */}
              <TabsContent value="ranking" className="space-y-4">
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                      <Trophy className="h-5 w-5" />
                      Ranking de Vendedores por Ventas
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-4">
                      {vendorRanking?.map((vendor: any, index: number) => (
                        <div
                          key={vendor.vendorId}
                          className="flex items-center justify-between p-4 border rounded-lg hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors"
                        >
                          <div className="flex items-center gap-4">
                            <Badge className={`${getRankBadgeColor(vendor.rank)} text-white`}>
                              <div className="flex items-center gap-1">
                                {getRankIcon(vendor.rank)}
                                #{vendor.rank}
                              </div>
                            </Badge>
                            
                            <div>
                              <h3 className="font-semibold text-lg">{vendor.vendorName}</h3>
                              <p className="text-sm text-gray-600 dark:text-gray-400">
                                {vendor.vendorPhone}
                              </p>
                            </div>
                          </div>

                          <div className="grid grid-cols-1 md:grid-cols-4 gap-4 text-center">
                            <div>
                              <p className="text-2xl font-bold text-green-600">
                                ${vendor.totalSales}
                              </p>
                              <p className="text-xs text-gray-500">Ventas Totales</p>
                            </div>
                            <div>
                              <p className="text-lg font-semibold">
                                {vendor.totalOrders}
                              </p>
                              <p className="text-xs text-gray-500">Órdenes</p>
                            </div>
                            <div>
                              <p className="text-lg font-semibold">
                                ${vendor.averageOrderValue}
                              </p>
                              <p className="text-xs text-gray-500">Promedio/Orden</p>
                            </div>
                            <div>
                              <p className="text-lg font-semibold text-blue-600">
                                ${vendor.estimatedProfit}
                              </p>
                              <p className="text-xs text-gray-500">Ganancia Est.</p>
                            </div>
                          </div>
                        </div>
                      ))}
                    </div>
                  </CardContent>
                </Card>
              </TabsContent>

              {/* Vendor Profits Tab */}
              <TabsContent value="profits" className="space-y-4">
                {vendorProfits?.map((vendor: any) => (
                  <Card key={vendor.vendorId}>
                    <CardHeader>
                      <CardTitle className="flex items-center justify-between">
                        <div className="flex items-center gap-2">
                          <TrendingUp className="h-5 w-5" />
                          {vendor.vendorName} - Reporte Detallado
                        </div>
                        <Badge variant="secondary">
                          {vendor.profitMargin}% margen
                        </Badge>
                      </CardTitle>
                    </CardHeader>
                    <CardContent>
                      {/* Summary metrics */}
                      <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
                        <div className="text-center p-4 bg-green-50 dark:bg-green-900/20 rounded-lg">
                          <p className="text-2xl font-bold text-green-600">
                            ${vendor.totalRevenue}
                          </p>
                          <p className="text-sm text-gray-600 dark:text-gray-400">Ingresos</p>
                        </div>
                        <div className="text-center p-4 bg-red-50 dark:bg-red-900/20 rounded-lg">
                          <p className="text-2xl font-bold text-red-600">
                            ${vendor.totalCost}
                          </p>
                          <p className="text-sm text-gray-600 dark:text-gray-400">Costos</p>
                        </div>
                        <div className="text-center p-4 bg-blue-50 dark:bg-blue-900/20 rounded-lg">
                          <p className="text-2xl font-bold text-blue-600">
                            ${vendor.totalProfit}
                          </p>
                          <p className="text-sm text-gray-600 dark:text-gray-400">Ganancia</p>
                        </div>
                        <div className="text-center p-4 bg-purple-50 dark:bg-purple-900/20 rounded-lg">
                          <p className="text-2xl font-bold text-purple-600">
                            {vendor.orderCount}
                          </p>
                          <p className="text-sm text-gray-600 dark:text-gray-400">Órdenes</p>
                        </div>
                      </div>

                      {/* Recent orders */}
                      <div>
                        <h4 className="font-semibold mb-4">Órdenes Recientes</h4>
                        <div className="space-y-2">
                          {vendor.orders?.slice(0, 5).map((order: any) => (
                            <div
                              key={order.orderId}
                              className="flex items-center justify-between p-3 border rounded hover:bg-gray-50 dark:hover:bg-gray-800"
                            >
                              <div>
                                <p className="font-medium">#{order.orderNumber}</p>
                                <p className="text-sm text-gray-600 dark:text-gray-400">
                                  {order.customerName} • {new Date(order.date).toLocaleDateString()}
                                </p>
                              </div>
                              <div className="text-right">
                                <p className="font-semibold text-green-600">
                                  +${order.profit}
                                </p>
                                <p className="text-sm text-gray-500">
                                  {order.profitMargin}% margen
                                </p>
                              </div>
                            </div>
                          ))}
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                ))}
              </TabsContent>
            </Tabs>
          </div>
        </div>
      </div>
    </div>
  );
}