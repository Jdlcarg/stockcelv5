import { useState, useEffect } from "react";
import { useAuth } from "@/hooks/use-auth";
import { useQuery } from "@tanstack/react-query";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { 
  DropdownMenu, 
  DropdownMenuContent, 
  DropdownMenuItem, 
  DropdownMenuSeparator, 
  DropdownMenuTrigger 
} from "@/components/ui/dropdown-menu";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import { ThemeToggle } from "@/components/ui/theme-toggle";
import ProfileModal from "@/components/profile-modal";
import { Smartphone, Menu, ChevronDown, LogOut, Settings, User, AlertTriangle, UserPlus } from "lucide-react";
import { Link } from "wouter";
import RealTimeClock from "@/components/layout/real-time-clock";

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

interface HeaderProps {
  onMobileMenuToggle: () => void;
}

export default function Header({ onMobileMenuToggle }: HeaderProps) {
  const { user, client, logout } = useAuth();
  const [clickSequence, setClickSequence] = useState<string[]>([]);
  const [showAdminAccess, setShowAdminAccess] = useState(false);
  const [showProfileModal, setShowProfileModal] = useState(false);

  // Hidden admin access: 5 clicks on "StockCel" logo in exact sequence 
  const SECRET_SEQUENCE = ['S', 'T', 'O', 'C', 'K'];

  // Query to get lost products for global alert
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

  const handleLogout = async () => {
    try {
      await logout();
      // Redirigir al login después de logout exitoso
      window.location.href = '/';
    } catch (error) {
      console.error('Error durante logout:', error);
      // Forzar logout local aunque falle el servidor
      window.location.href = '/';
    }
  };

  const handleLogoClick = () => {
    if (user?.role !== 'superuser') return;

    const nextIndex = clickSequence.length;
    const nextExpectedChar = SECRET_SEQUENCE[nextIndex];

    setClickSequence(prev => [...prev, nextExpectedChar]);

    if (clickSequence.length + 1 === SECRET_SEQUENCE.length) {
      setShowAdminAccess(true);
      setTimeout(() => {
        setShowAdminAccess(false);
        setClickSequence([]);
      }, 10000); // Show for 10 seconds
    } else if (clickSequence.length + 1 > SECRET_SEQUENCE.length) {
      setClickSequence([]);
    }
  };

  // Reset sequence after 3 seconds of inactivity
  useEffect(() => {
    if (clickSequence.length > 0) {
      const timer = setTimeout(() => {
        setClickSequence([]);
      }, 3000);
      return () => clearTimeout(timer);
    }
  }, [clickSequence]);

  return (
    <header className="bg-white dark:bg-gray-900 shadow-sm border-b border-gray-200 dark:border-gray-700 sticky top-0 z-30">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between items-center py-4">
          <div className="flex items-center">
            <Button
              variant="ghost"
              size="sm"
              className="lg:hidden p-2"
              onClick={onMobileMenuToggle}
            >
              <Menu className="h-4 w-4" />
            </Button>

            <div className="flex items-center ml-2 lg:ml-0">
              <div 
                className="h-8 w-8 bg-primary rounded-lg flex items-center justify-center cursor-pointer transition-transform hover:scale-110"
                onClick={handleLogoClick}
              >
                <Smartphone className="h-4 w-4 text-white" />
              </div>
              <span 
                className="ml-2 text-xl font-bold text-gray-900 dark:text-white cursor-pointer select-none"
                onClick={handleLogoClick}
              >
                StockCel
              </span>

              {/* Hidden Admin Access Button */}
              {showAdminAccess && user?.role === 'superuser' && (
                <Link href="/admin/create">
                  <Button
                    variant="outline"
                    size="sm"
                    className="ml-4 animate-pulse border-blue-500 text-blue-700 hover:bg-blue-50 dark:border-blue-400 dark:text-blue-400 dark:hover:bg-blue-950/20"
                  >
                    <UserPlus className="h-4 w-4 mr-2" />
                    <span className="text-sm font-medium">
                      Crear Admin
                    </span>
                  </Button>
                </Link>
              )}
            </div>
          </div>

          <div className="flex items-center space-x-4">
            {/* Global Lost Products Alert */}
            {hasLostProducts && (
              <Link href="/stock-control">
                <Button
                  variant="outline"
                  size="sm"
                  className="animate-pulse border-red-500 text-red-700 hover:bg-red-50 dark:border-red-400 dark:text-red-400 dark:hover:bg-red-950/20 hidden sm:flex"
                >
                  <AlertTriangle className="h-4 w-4 mr-2" />
                  <span className="text-sm font-medium">
                    {lostProducts.length} Extravío{lostProducts.length > 1 ? 's' : ''}
                  </span>
                  <Badge variant="destructive" className="ml-2 h-5 w-5 p-0 flex items-center justify-center text-xs">
                    {lostProducts.length}
                  </Badge>
                </Button>
              </Link>
            )}

            {/* Mobile Lost Products Alert - Only icon */}
            {hasLostProducts && (
              <Link href="/stock-control">
                <Button
                  variant="outline"
                  size="sm"
                  className="animate-pulse border-red-500 text-red-700 hover:bg-red-50 dark:border-red-400 dark:text-red-400 dark:hover:bg-red-950/20 sm:hidden relative"
                >
                  <AlertTriangle className="h-4 w-4" />
                  <Badge variant="destructive" className="absolute -top-2 -right-2 h-5 w-5 p-0 flex items-center justify-center text-xs">
                    {lostProducts.length}
                  </Badge>
                </Button>
              </Link>
            )}

            <div className="flex items-center space-x-2">
              <span className="text-sm text-gray-700 dark:text-gray-300">{client?.name}</span>
              <span className="text-xs text-gray-500 dark:text-gray-400">{user?.role}</span>
            </div>

            {/* Real-time clock */}
            <RealTimeClock showDate={true} />
            <ThemeToggle />

            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button variant="ghost" className="flex items-center space-x-2">
                  <Avatar className="h-8 w-8">
                    <AvatarFallback className="bg-gray-300 dark:bg-gray-700 text-gray-600 dark:text-gray-300">
                      {user?.username.charAt(0).toUpperCase()}
                    </AvatarFallback>
                  </Avatar>
                  <span className="text-sm font-medium text-gray-900 dark:text-white">{user?.username}</span>
                  <ChevronDown className="h-4 w-4" />
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end">
                <DropdownMenuItem onClick={() => setShowProfileModal(true)}>
                  <User className="mr-2 h-4 w-4" />
                  Perfil
                </DropdownMenuItem>
                <DropdownMenuSeparator />
                <DropdownMenuItem onClick={handleLogout}>
                  <LogOut className="mr-2 h-4 w-4" />
                  Cerrar Sesión
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          </div>
        </div>
      </div>

      {/* Modal de Perfil */}
      <ProfileModal 
        open={showProfileModal} 
        onOpenChange={setShowProfileModal} 
      />
    </header>
  );
}