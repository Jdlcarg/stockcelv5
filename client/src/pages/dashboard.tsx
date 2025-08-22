import { useEffect, useState } from "react";
import { useLocation } from "wouter";
import { useAuth } from "@/hooks/use-auth";
import { useQuery } from "@tanstack/react-query";
import Header from "@/components/layout/header";
import Sidebar from "@/components/layout/sidebar";
import MobileNav from "@/components/layout/mobile-nav";
import Footer from "@/components/layout/footer";
import StatsCards from "@/components/dashboard/stats-cards";
import QuickActions from "@/components/dashboard/quick-actions";
import OrderTable from "@/components/orders/order-table";
import { SubscriptionStatus } from "@/components/subscription-status";
import { useSubscriptionGuard } from "@/hooks/use-subscription-guard";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Package, ShoppingCart, Users, Settings, CreditCard, TrendingUp, Plus, Eye, Calculator } from "lucide-react";

export default function Dashboard() {
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const [, setLocation] = useLocation();
  const { isAuthenticated, user, client } = useAuth();

  // Subscription guard
  const { isBlocked, SubscriptionBlockedDialog } = useSubscriptionGuard({
    client: client || { subscriptionType: "trial", trialEndDate: undefined, salesContactNumber: undefined },
    user: user || { role: "vendor" }
  });

  useEffect(() => {
    if (!isAuthenticated) {
      setLocation("/");
    } else if (user?.role === 'superuser') {
      // Redirect SuperUser to their specific dashboard
      setLocation("/admin/dashboard");
    } else if (user?.role === 'vendor') {
      // Check if vendor has dashboard permission
      const userPermissions = user.permissions || [];
      if (!userPermissions.includes('dashboard')) {
        // Redirect to the first available section with permission
        const availableRoutes = [
          { permission: 'products', route: '/products' },
          { permission: 'orders', route: '/orders' },
          { permission: 'clients', route: '/customers' },
          { permission: 'cash_register', route: '/cash-advanced' },
          { permission: 'reports', route: '/reportes' },
          { permission: 'payments_expenses', route: '/pagos-gastos' },
          { permission: 'stock_control', route: '/stock-control' }
        ];
        
        const firstAvailableRoute = availableRoutes.find(route => 
          userPermissions.includes(route.permission)
        );
        
        if (firstAvailableRoute) {
          setLocation(firstAvailableRoute.route);
        } else {
          // No permissions available, redirect to a basic info page or logout
          setLocation("/");
        }
        return;
      }
    }
  }, [isAuthenticated, user, setLocation]);

  const { data: stats } = useQuery({
    queryKey: ["/api/dashboard/stats"],
    queryFn: async () => {
      const response = await fetch(`/api/dashboard/stats?clientId=${user?.clientId}`);
      return response.json();
    },
    enabled: !!user?.clientId,
  });

  const { data: orders } = useQuery({
    queryKey: ["/api/orders"],
    queryFn: async () => {
      const response = await fetch(`/api/orders?clientId=${user?.clientId}`);
      return response.json();
    },
    enabled: !!user?.clientId,
  });

  if (!isAuthenticated) {
    return null;
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 to-blue-50 dark:from-gray-900 dark:to-gray-800 flex flex-col">
      <Header onMobileMenuToggle={() => setMobileNavOpen(true)} />
      
      {/* Subscription Blocked Dialog */}
      <SubscriptionBlockedDialog />
      
      {/* Disable all interactions if subscription is expired */}
      <div className={isBlocked ? "pointer-events-none opacity-50" : ""}>
        <MobileNav open={mobileNavOpen} onOpenChange={setMobileNavOpen} />
      
      <div className="flex flex-1">
        <Sidebar />
        
        <main className="flex-1">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
            
            {/* Header Section */}
            <div className="mb-8">
              <div className="flex items-center justify-between mb-6">
                <div>
                  <h1 className="text-3xl font-bold text-gray-900 dark:text-white">Dashboard</h1>
                  <p className="text-gray-600 dark:text-gray-300 mt-1">Bienvenido de vuelta, {user?.username}</p>
                </div>
                <div className="flex items-center space-x-4">
                  <Select defaultValue="7">
                    <SelectTrigger className="w-48">
                      <SelectValue placeholder="Seleccionar período" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="7">Últimos 7 días</SelectItem>
                      <SelectItem value="30">Últimos 30 días</SelectItem>
                      <SelectItem value="90">Últimos 90 días</SelectItem>
                    </SelectContent>
                  </Select>
                  <Button 
                    onClick={() => setLocation("/orders")}
                    className="gradient-bg text-white hover:opacity-90"
                  >
                    <Plus className="mr-2 h-4 w-4" />
                    Nueva Venta
                  </Button>
                </div>
              </div>
              
              {/* Subscription Status - Only show for non-superuser */}
              {user?.role !== "superuser" && client && (
                <div className="mb-6">
                  <SubscriptionStatus 
                    client={{
                      ...client,
                      subscriptionType: client.subscriptionType || "trial",
                      trialStartDate: client.trialStartDate,
                      trialEndDate: client.trialEndDate,
                      salesContactNumber: client.salesContactNumber
                    }} 
                    user={user}
                  />
                </div>
              )}
              
              <StatsCards stats={stats} />
            </div>

            {/* Quick Actions */}
            <div className="mb-8">
              <h2 className="text-xl font-semibold text-gray-900 dark:text-white mb-4">Accesos Rápidos</h2>
              <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-4">
                <Button
                  variant="outline"
                  className="h-20 flex flex-col items-center justify-center space-y-2 card-hover"
                  onClick={() => setLocation("/products")}
                >
                  <Package className="h-6 w-6 text-primary" />
                  <span className="text-sm">Productos</span>
                </Button>
                
                <Button
                  variant="outline"
                  className="h-20 flex flex-col items-center justify-center space-y-2 card-hover"
                  onClick={() => setLocation("/orders")}
                >
                  <ShoppingCart className="h-6 w-6 text-accent" />
                  <span className="text-sm">Pedidos</span>
                </Button>
                
                <Button
                  variant="outline"
                  className="h-20 flex flex-col items-center justify-center space-y-2 card-hover"
                  onClick={() => setLocation("/customers")}
                >
                  <Users className="h-6 w-6 text-blue-600" />
                  <span className="text-sm">Clientes</span>
                </Button>
                
                <Button
                  variant="outline"
                  className="h-20 flex flex-col items-center justify-center space-y-2 card-hover"
                  onClick={() => setLocation("/cash")}
                >
                  <CreditCard className="h-6 w-6 text-green-600" />
                  <span className="text-sm">Caja</span>
                </Button>
                
                <Button
                  variant="outline"
                  className="h-20 flex flex-col items-center justify-center space-y-2 card-hover"
                  onClick={() => setLocation("/currency-converter")}
                >
                  <Calculator className="h-6 w-6 text-yellow-600" />
                  <span className="text-sm">Conversión</span>
                </Button>
                
                <Button
                  variant="outline"
                  className="h-20 flex flex-col items-center justify-center space-y-2 card-hover"
                  onClick={() => setLocation("/settings")}
                >
                  <Settings className="h-6 w-6 text-gray-600" />
                  <span className="text-sm">Configuración</span>
                </Button>
              </div>
            </div>

            {/* Recent Orders */}
            <Card className="card-hover">
              <CardHeader>
                <div className="flex items-center justify-between">
                  <CardTitle className="flex items-center text-xl">
                    <TrendingUp className="mr-2 h-5 w-5" />
                    Pedidos Recientes
                  </CardTitle>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => setLocation("/orders")}
                    className="text-primary hover:text-primary/80"
                  >
                    <Eye className="mr-1 h-4 w-4" />
                    Ver todos
                  </Button>
                </div>
              </CardHeader>
              <CardContent>
                {orders && orders.length > 0 ? (
                  <OrderTable orders={orders.slice(0, 5)} />
                ) : (
                  <div className="text-center py-8 text-gray-500">
                    <ShoppingCart className="mx-auto h-12 w-12 mb-4 text-gray-300" />
                    <p className="text-lg font-medium">No hay pedidos recientes</p>
                    <p className="text-sm">Los pedidos aparecerán aquí cuando realices ventas</p>
                    <Button
                      onClick={() => setLocation("/orders")}
                      className="mt-4 gradient-bg text-white"
                    >
                      <Plus className="mr-2 h-4 w-4" />
                      Crear Primera Venta
                    </Button>
                  </div>
                )}
              </CardContent>
            </Card>
            
          </div>
        </main>
      </div>
      <Footer />
      </div>
    </div>
  );
}
