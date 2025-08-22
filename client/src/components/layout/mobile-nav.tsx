import { Link, useLocation } from "wouter";
import { cn } from "@/lib/utils";
import { useAuth } from "@/hooks/use-auth";
import { useQuery } from "@tanstack/react-query";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Sheet, SheetContent, SheetHeader, SheetTitle } from "@/components/ui/sheet";
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
  FileBarChart
} from "lucide-react";

const navigation = [
  { name: "Dashboard", href: "/dashboard", icon: Home },
  { name: "Productos", href: "/products", icon: Smartphone },
  { name: "Pedidos", href: "/orders", icon: ShoppingCart },
  { name: "Control Stock", href: "/stock-control", icon: Search },
  { name: "Cajas Avanzadas", href: "/cash-advanced", icon: Calculator },
  { name: "Pagos/Gastos", href: "/pagos-gastos", icon: TrendingUp },
  { name: "Reportes", href: "/reportes", icon: FileBarChart },
  { name: "Conversión", href: "/currency-converter", icon: ArrowUpDown },
  { name: "Vendedores", href: "/vendors", icon: UserCheck },
  { name: "Clientes", href: "/customers", icon: UserPlus },
  { name: "Usuarios", href: "/users", icon: Users },
  { name: "Configuración", href: "/settings", icon: Settings },
];

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

interface MobileNavProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export default function MobileNav({ open, onOpenChange }: MobileNavProps) {
  const [location] = useLocation();
  const { user } = useAuth();

  // Query to get lost products for mobile nav alert
  const { data: products } = useQuery<Product[]>({
    queryKey: ['/api/products', user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/products?clientId=${user?.clientId}`);
      return response.json();
    },
    enabled: !!user?.clientId,
  });

  const lostProducts = products?.filter(p => p.status === 'extravio') || [];
  const hasLostProducts = lostProducts.length > 0;

  // Filter navigation items based on user role
  const filteredNavigation = navigation.filter(item => {
    // Only show Users section for superuser (developer)
    if (item.href === "/users") {
      return user?.role === "superuser";
    }
    return true;
  });

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent side="left" className="w-full max-w-xs">
        <SheetHeader>
          <SheetTitle>StockCel</SheetTitle>
        </SheetHeader>
        
        <nav className="mt-6">
          <ul className="space-y-1">
            {filteredNavigation.map((item) => {
              const Icon = item.icon;
              const isActive = location === item.href;
              const isStockControl = item.href === "/stock-control";
              
              return (
                <li key={item.name}>
                  <Link href={item.href}>
                    <div
                      className={cn(
                        "group flex items-center px-3 py-3 text-sm font-medium rounded-xl transition-all duration-200 ease-in-out cursor-pointer relative",
                        isActive
                          ? "bg-gradient-to-r from-primary/20 to-primary/10 text-primary shadow-sm border-l-4 border-primary"
                          : "text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 hover:text-primary hover:shadow-sm",
                        isStockControl && hasLostProducts && "animate-pulse border-red-500/30"
                      )}
                      onClick={() => onOpenChange(false)}
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
        </nav>
      </SheetContent>
    </Sheet>
  );
}
