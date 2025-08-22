import { Link, useLocation } from "wouter";
import { cn } from "@/lib/utils";
import { useAuth } from "@/hooks/use-auth";
import { useQuery } from "@tanstack/react-query";
import { Badge } from "@/components/ui/badge";
import {
  BarChart3,
  Smartphone,
  ShoppingCart,
  CreditCard,
  Users,
  Settings,
  Home,
  UserCheck,
  UserPlus,
  ArrowUpDown,
  Search,
  AlertTriangle,
  Calculator,
  TrendingUp,
  FileBarChart,
  Shield,
  Activity,
  Clock // Import Clock icon
} from "lucide-react";

const getNavigation = (userRole: string) => {
  // SuperUser navigation - only admin management and resellers
  if (userRole === 'superuser') {
    return [
      { name: "Panel SuperUser", href: "/admin/dashboard", icon: Shield },
      { name: "Crear Admin", href: "/admin/create", icon: UserPlus },
      { name: "Gestión de Revendedores", href: "/admin/resellers-management", icon: Users },
    ];
  }

  // Reseller navigation - only reseller dashboard
  if (userRole === 'reseller') {
    return [
      { name: "Dashboard Revendedor", href: "/reseller/dashboard", icon: Home },
    ];
  }

  // Admin and Vendor navigation - full system access
  return [
    { name: "Dashboard", href: "/dashboard", icon: Home },
    { name: "Productos", href: "/products", icon: Smartphone },
    { name: "Pedidos", href: "/orders", icon: ShoppingCart },
    { name: "Control Stock", href: "/stock-control", icon: Search },
    { name: "Cajas Avanzadas", href: "/cash-advanced", icon: Calculator },
    { name: "Pagos/Gastos", href: "/pagos-gastos", icon: TrendingUp },
    { name: "Reportes", href: "/reportes", icon: FileBarChart },
    { name: "Monitor Automático", href: "/auto-sync-monitor", icon: Activity },
    { name: "Conversión", href: "/currency-converter", icon: ArrowUpDown },
    { name: "Vendedores", href: "/vendors", icon: UserCheck },
    { name: "Clientes", href: "/customers", icon: UserPlus },
    { name: "Usuarios", href: "/users", icon: Users },
    { name: "Configuración", href: "/settings", icon: Settings },
    // Add new navigation item for cash schedules
    { name: "Horarios de Caja", href: "/cash-schedule", icon: Clock },
  ];
};

interface Product {
  id: number;
  clientId: number;
  imei: string;
  model: string;
  storage: string;
  color: string;
  status: 'disponible' | 'reservado' | 'vendido' | 'tecnico_interno' | 'tecnico_externo' | 'a_reparar' | 'extravio';
  costPrice: number;
  salePrice?: number;
  observations?: string;
  createdAt: string;
  updatedAt?: string;
  entryDate?: string;
}

export default function Sidebar() {
  const [location] = useLocation();
  const { user } = useAuth();

  // Get navigation based on user role
  const navigation = getNavigation(user?.role || 'vendor');

  // Query to get lost products for sidebar alert (only for non-superuser)
  const { data: products } = useQuery<Product[]>({
    queryKey: ['/api/products', user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/products?clientId=${user?.clientId}`);
      return response.json();
    },
    enabled: !!user?.clientId && user?.role !== 'superuser',
  });

  const lostProducts = products?.filter(p => p.status === 'extravio') || [];
  const hasLostProducts = lostProducts.length > 0;

  // Helper function to check permissions
  const hasPermission = (permission: string) => {
    if (!user) return false;
    if (user.role === "superuser" || user.role === "admin") return true;
    if (user.role === "vendor") {
      const userPermissions = user.permissions || [];
      return userPermissions.includes(permission);
    }
    return false;
  };

  // Filter navigation items based on user role and permissions (only for non-superuser)
  const filteredNavigation = user?.role === 'superuser' ? navigation : navigation.filter(item => {
    // Only show Users section for superuser (developer)
    if (item.href === "/users") {
      return user?.role === "superuser";
    }

    // Permission-based filtering for vendors
    switch (item.href) {
      case "/dashboard":
        return user?.role === "admin" || user?.role === "superuser" || hasPermission("dashboard");
      case "/products":
        return hasPermission("products");
      case "/orders":
        return hasPermission("orders");
      case "/customers":
        return hasPermission("clients");
      case "/cash-advanced":
        return hasPermission("cash_register");
      case "/reportes":
        return hasPermission("reports");
      case "/pagos-gastos":
        return hasPermission("payments_expenses");
      case "/auto-sync-monitor":
        return user?.role === "admin" || user?.role === "superuser";
      case "/stock-control":
        return hasPermission("stock_control");
      case "/vendors":
        return user?.role === "admin" || user?.role === "superuser";
      case "/currency-converter":
        return hasPermission("cash_register");
      case "/settings":
        return user?.role === "admin" || user?.role === "superuser";
      case "/cash-schedule": // Add permission check for the new route
        return hasPermission("settings"); // Assuming settings permission covers cash schedule
      default:
        return true; // Default access
    }
  });

  return (
    <nav className="hidden lg:block w-64 bg-white dark:bg-gray-900 shadow-lg h-screen sticky top-16 overflow-y-auto border-r border-gray-200 dark:border-gray-700">
      <div className="py-6">
        <div className="px-4 mb-6">
          <h2 className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider">
            StockCel
          </h2>
        </div>
        <ul className="space-y-1 px-3">
          {filteredNavigation.map((item) => {
            const Icon = item.icon;
            const isActive = location === item.href;
            const isStockControl = item.href === "/stock-control";

            return (
              <li key={item.name}>
                <Link href={item.href}>
                  <div
                    className={cn(
                      "group flex items-center px-4 py-3 text-sm font-medium rounded-xl transition-all duration-200 ease-in-out cursor-pointer relative",
                      isActive
                        ? "bg-gradient-to-r from-primary/20 to-primary/10 text-primary shadow-sm border-l-4 border-primary"
                        : "text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 hover:text-primary hover:shadow-sm",
                      isStockControl && hasLostProducts && "animate-pulse border-red-500/30"
                    )}
                  >
                    <Icon className={cn(
                      "mr-3 h-5 w-5 transition-colors",
                      isActive ? "text-primary" : "text-gray-500 dark:text-gray-400 group-hover:text-primary",
                      isStockControl && hasLostProducts && "text-red-500 dark:text-red-400"
                    )} />
                    {item.name}

                    {/* Lost products badge for Stock Control */}
                    {isStockControl && hasLostProducts && (
                      <>
                        <div className="flex-1" />
                        <div className="flex items-center space-x-1">
                          <AlertTriangle className="h-3 w-3 text-red-500" />
                          <Badge variant="destructive" className="h-4 text-xs px-1">
                            {lostProducts.length}
                          </Badge>
                        </div>
                      </>
                    )}
                  </div>
                </Link>
              </li>
            );
          })}
        </ul>
      </div>
    </nav>
  );
}