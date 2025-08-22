import { useEffect, useState } from "react";
import { useLocation } from "wouter";
import { useAuth } from "@/hooks/use-auth";
import { useQuery } from "@tanstack/react-query";
import Header from "@/components/layout/header";
import Sidebar from "@/components/layout/sidebar";
import MobileNav from "@/components/layout/mobile-nav";
import Footer from "@/components/layout/footer";
import OrderTable from "@/components/orders/order-table";


import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Plus } from "lucide-react";

export default function Orders() {
  const [mobileNavOpen, setMobileNavOpen] = useState(false);


  const [, setLocation] = useLocation();
  const { isAuthenticated, user } = useAuth();

  useEffect(() => {
    if (!isAuthenticated) {
      setLocation("/");
    }
  }, [isAuthenticated, setLocation]);

  const { data: orders, isLoading } = useQuery({
    queryKey: ["/api/orders", user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/orders?clientId=${user?.clientId}`, {
        headers: {
          'x-user-id': user?.id?.toString() || '',
        },
      });
      if (!response.ok) {
        throw new Error('Failed to fetch orders');
      }
      return response.json();
    },
    enabled: !!user?.clientId,
  });

  if (!isAuthenticated) {
    return null;
  }

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900 flex flex-col">
      <Header onMobileMenuToggle={() => setMobileNavOpen(true)} />
      <MobileNav open={mobileNavOpen} onOpenChange={setMobileNavOpen} />
      
      <div className="flex flex-1">
        <Sidebar />
        
        <main className="flex-1 bg-gray-50 dark:bg-gray-900">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
            
            <Card>
              <CardHeader>
                <div className="flex items-center justify-between">
                  <CardTitle className="text-gray-900 dark:text-white">Gestión de Pedidos</CardTitle>
                  <Button onClick={() => setLocation("/create-order")}>
                    <Plus className="mr-2 h-4 w-4" />
                    Crear Pedido
                  </Button>
                </div>
              </CardHeader>
              <CardContent>
                <OrderTable orders={orders || []} />
              </CardContent>
            </Card>
            
          </div>
        </main>
      </div>
      <Footer />
    </div>
  );
}
