import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Order } from "@shared/schema";
import { useLocation } from "wouter";
import { FileText, Search, Filter, X, ChevronUp, ChevronDown } from "lucide-react";
import { useState, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { useAuth } from "@/hooks/use-auth";

type SortField = 'orderNumber' | 'customerName' | 'vendorId' | 'totalUsd' | 'status' | 'createdAt';
type SortDirection = 'asc' | 'desc';

interface OrderTableProps {
  orders: Order[];
}

export default function OrderTable({ orders }: OrderTableProps) {
  const [, setLocation] = useLocation();
  const { user } = useAuth();
  
  // Estados para filtros
  const [searchTerm, setSearchTerm] = useState("");
  const [selectedVendor, setSelectedVendor] = useState("all");
  const [selectedCustomer, setSelectedCustomer] = useState("all");
  const [selectedStatus, setSelectedStatus] = useState("all");
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo, setDateTo] = useState("");
  const [showFilters, setShowFilters] = useState(false);
  
  // Estados para ordenamiento
  const [sortField, setSortField] = useState<SortField>('createdAt');
  const [sortDirection, setSortDirection] = useState<SortDirection>('desc');

  // Obtener vendedores
  const { data: vendors } = useQuery({
    queryKey: ["/api/users/vendors", user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/users/vendors?clientId=${user?.clientId}`, {
        headers: {
          'x-user-id': user?.id?.toString() || '',
        },
      });
      if (!response.ok) {
        // Si falla la petición, devolvemos array vacío para evitar errores
        return [];
      }
      return response.json();
    },
    enabled: !!user?.clientId && !!user?.id,
  });

  // Obtener clientes únicos de los pedidos
  const uniqueCustomers = useMemo(() => {
    if (!orders || orders.length === 0) return [];
    const customerMap = new Map();
    orders.forEach(order => {
      if (order.customerName && !customerMap.has(order.customerName)) {
        customerMap.set(order.customerName, {
          name: order.customerName,
          id: order.customerId
        });
      }
    });
    return Array.from(customerMap.values());
  }, [orders]);
  
  const getPaymentStatusBadge = (order: Order) => {
    const status = getPaymentStatus(order);
    switch (status) {
      case "pagado":
        return <Badge className="bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200">Pagado</Badge>;
      case "pendiente":
        return <Badge className="bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200">Pendiente</Badge>;
      case "parcial":
        return <Badge className="bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200">Parcial</Badge>;
      default:
        return <Badge variant="secondary">{status}</Badge>;
    }
  };

  // Función para obtener nombre del vendedor
  const getVendorName = (order: any) => {
    // Primero intentar usar vendorName directamente desde la orden (nuevo formato API)
    if (order.vendorName) {
      return order.vendorName;
    }
    
    // Fallback: buscar en la lista de vendors por vendorId (formato anterior)
    if (order.vendorId && vendors && Array.isArray(vendors)) {
      const vendor = vendors.find((v: any) => v.id === order.vendorId);
      return vendor ? vendor.username || vendor.name : "Usuario desconocido";
    }
    
    return "Usuario desconocido";
  };

  // Función para obtener el estado de pago real de la base de datos
  const getPaymentStatus = (order: Order) => {
    // Usar el paymentStatus real de la base de datos
    if (order.paymentStatus === 'pagado') return "pagado";
    if (order.paymentStatus === 'parcial') return "parcial";
    return "pendiente"; // Para 'no_pagado' o valores null/undefined
  };

  // Filtros y ordenamiento aplicados
  const filteredAndSortedOrders = useMemo(() => {
    // Primero filtrar
    let filtered = orders.filter(order => {
      // Filtro por término de búsqueda (pedido, cliente)
      const matchesSearch = !searchTerm || 
        order.orderNumber.toLowerCase().includes(searchTerm.toLowerCase()) ||
        order.customerName.toLowerCase().includes(searchTerm.toLowerCase());

      // Filtro por vendedor
      const matchesVendor = !selectedVendor || selectedVendor === "all" || order.vendorId?.toString() === selectedVendor;

      // Filtro por cliente
      const matchesCustomer = !selectedCustomer || selectedCustomer === "all" || order.customerName === selectedCustomer;

      // Filtro por estado de pago
      const paymentStatus = getPaymentStatus(order);
      const matchesStatus = !selectedStatus || selectedStatus === "all" || paymentStatus === selectedStatus;

      // Filtro por fecha
      const orderDate = new Date(order.createdAt || Date.now());
      const matchesDateFrom = !dateFrom || orderDate >= new Date(dateFrom);
      const matchesDateTo = !dateTo || orderDate <= new Date(dateTo + "T23:59:59");

      return matchesSearch && matchesVendor && matchesCustomer && matchesStatus && matchesDateFrom && matchesDateTo;
    });

    // Luego ordenar
    filtered.sort((a, b) => {
      let aValue: any = a[sortField];
      let bValue: any = b[sortField];

      // Manejo especial para vendorId (obtener el nombre)
      if (sortField === 'vendorId') {
        aValue = getVendorName(a);
        bValue = getVendorName(b);
      }

      // Manejo especial para fechas
      if (sortField === 'createdAt') {
        aValue = new Date(aValue).getTime();
        bValue = new Date(bValue).getTime();
      }

      // Manejo especial para números
      if (sortField === 'totalUsd') {
        aValue = parseFloat(aValue) || 0;
        bValue = parseFloat(bValue) || 0;
      }

      // Para strings, convertir a lowercase
      if (typeof aValue === 'string') {
        aValue = aValue.toLowerCase();
      }
      if (typeof bValue === 'string') {
        bValue = bValue.toLowerCase();
      }

      if (aValue < bValue) return sortDirection === 'asc' ? -1 : 1;
      if (aValue > bValue) return sortDirection === 'asc' ? 1 : -1;
      return 0;
    });

    return filtered;
  }, [orders, searchTerm, selectedVendor, selectedCustomer, selectedStatus, dateFrom, dateTo, sortField, sortDirection, vendors]);

  // Función para manejar el ordenamiento
  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortDirection('asc');
    }
  };

  // Componente para el encabezado ordenable
  const SortableHeader = ({ field, children }: { field: SortField; children: React.ReactNode }) => (
    <TableHead 
      className="cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-800 select-none"
      onClick={() => handleSort(field)}
    >
      <div className="flex items-center gap-1">
        {children}
        {sortField === field && (
          sortDirection === 'asc' ? 
            <ChevronUp className="w-4 h-4" /> : 
            <ChevronDown className="w-4 h-4" />
        )}
      </div>
    </TableHead>
  );

  // Función para limpiar filtros
  const clearFilters = () => {
    setSearchTerm("");
    setSelectedVendor("all");
    setSelectedCustomer("all");
    setSelectedStatus("all");
    setDateFrom("");
    setDateTo("");
  };

  if (orders.length === 0) {
    return (
      <div className="text-center py-8">
        <p className="text-gray-500">No hay pedidos disponibles</p>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {/* Barra de búsqueda y filtros */}
      <div className="space-y-4">
        <div className="flex flex-col sm:flex-row gap-4 items-start sm:items-center justify-between">
          {/* Búsqueda principal */}
          <div className="relative flex-1 max-w-md">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
            <Input
              placeholder="Buscar por pedido o cliente..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="pl-10"
            />
          </div>
          
          {/* Botón de filtros avanzados */}
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              onClick={() => setShowFilters(!showFilters)}
              className="flex items-center gap-2"
            >
              <Filter className="w-4 h-4" />
              Filtros Avanzados
            </Button>
            
            {(searchTerm || (selectedVendor && selectedVendor !== "all") || (selectedCustomer && selectedCustomer !== "all") || (selectedStatus && selectedStatus !== "all") || dateFrom || dateTo) && (
              <Button
                variant="ghost"
                onClick={clearFilters}
                className="flex items-center gap-2 text-gray-500"
              >
                <X className="w-4 h-4" />
                Limpiar
              </Button>
            )}
          </div>
        </div>

        {/* Filtros avanzados */}
        {showFilters && (
          <div className="bg-gray-50 dark:bg-gray-800 p-4 rounded-lg space-y-4">
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
              {/* Filtro por vendedor */}
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                  Vendedor
                </label>
                <Select value={selectedVendor} onValueChange={setSelectedVendor}>
                  <SelectTrigger>
                    <SelectValue placeholder="Todos los vendedores" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">Todos los vendedores</SelectItem>
                    {vendors?.map((vendor: any) => (
                      <SelectItem key={vendor.id} value={vendor.id.toString()}>
                        {vendor.username || vendor.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              {/* Filtro por cliente */}
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                  Cliente
                </label>
                <Select value={selectedCustomer} onValueChange={setSelectedCustomer}>
                  <SelectTrigger>
                    <SelectValue placeholder="Todos los clientes" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">Todos los clientes</SelectItem>
                    {uniqueCustomers.map((customer: any) => (
                      <SelectItem key={customer.name} value={customer.name}>
                        {customer.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              {/* Filtro por estado de pago */}
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                  Estado de Pago
                </label>
                <Select value={selectedStatus} onValueChange={setSelectedStatus}>
                  <SelectTrigger>
                    <SelectValue placeholder="Todos los estados" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">Todos los estados</SelectItem>
                    <SelectItem value="pagado">Pagado</SelectItem>
                    <SelectItem value="pendiente">Pendiente</SelectItem>
                    <SelectItem value="parcial">Parcial</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              {/* Filtro fecha desde */}
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                  Fecha Desde
                </label>
                <Input
                  type="date"
                  value={dateFrom}
                  onChange={(e) => setDateFrom(e.target.value)}
                />
              </div>

              {/* Filtro fecha hasta */}
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                  Fecha Hasta
                </label>
                <Input
                  type="date"
                  value={dateTo}
                  onChange={(e) => setDateTo(e.target.value)}
                />
              </div>
            </div>
          </div>
        )}

        {/* Contador de resultados */}
        {filteredAndSortedOrders.length !== orders.length && (
          <div className="text-sm text-gray-600 dark:text-gray-400">
            Mostrando {filteredAndSortedOrders.length} de {orders.length} pedidos
          </div>
        )}
      </div>

      <div className="overflow-x-auto">
        <Table>
        <TableHeader>
          <TableRow>
            <SortableHeader field="orderNumber">Pedido</SortableHeader>
            <SortableHeader field="customerName">Cliente</SortableHeader>
            <SortableHeader field="vendorId">Vendedor</SortableHeader>
            <SortableHeader field="totalUsd">Total</SortableHeader>
            <SortableHeader field="status">Estado</SortableHeader>
            <TableHead>Entrega</TableHead>
            <TableHead>Observaciones</TableHead>
            <SortableHeader field="createdAt">Fecha</SortableHeader>
            <TableHead>Acciones</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {filteredAndSortedOrders.map((order) => (
            <TableRow key={order.id}>
              <TableCell className="font-medium">{order.orderNumber}</TableCell>
              <TableCell>{order.customerName}</TableCell>
              <TableCell className="text-sm text-gray-600 dark:text-gray-400">
                {getVendorName(order)}
              </TableCell>
              <TableCell>${parseFloat(order.totalUsd).toFixed(2)}</TableCell>
              <TableCell>{getPaymentStatusBadge(order)}</TableCell>
              <TableCell className="text-sm">
                {order.shippingType === 'oficina' && (
                  <Badge variant="outline" className="bg-blue-50 text-blue-700 border-blue-200 dark:bg-blue-900 dark:text-blue-200">
                    Retiro Oficina
                  </Badge>
                )}
                {order.shippingType === 'direccion' && (
                  <div className="space-y-1">
                    <Badge variant="outline" className="bg-green-50 text-green-700 border-green-200 dark:bg-green-900 dark:text-green-200">
                      Envío a Domicilio
                    </Badge>
                    {order.shippingAddress && (
                      <div className="text-xs text-gray-600 dark:text-gray-400 truncate max-w-xs" title={order.shippingAddress}>
                        {order.shippingAddress}
                      </div>
                    )}
                  </div>
                )}
                {!order.shippingType && (
                  <span className="text-gray-400 italic">No especificado</span>
                )}
              </TableCell>
              <TableCell className="max-w-xs">
                <div className="text-sm text-gray-600 dark:text-gray-400 truncate" title={order.observations || ""}>
                  {order.observations ? (
                    order.observations.length > 40 ? 
                      `${order.observations.substring(0, 40)}...` : 
                      order.observations
                  ) : (
                    <span className="italic text-gray-400">Sin observaciones</span>
                  )}
                </div>
              </TableCell>
              <TableCell>{new Date(order.createdAt || Date.now()).toLocaleDateString()}</TableCell>
              <TableCell>
                <div className="flex space-x-2">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => setLocation(`/invoice/${order.id}`)}
                    className="h-8 px-3"
                  >
                    <FileText className="w-4 h-4 mr-1" />
                    Factura
                  </Button>
                </div>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
      </div>
    </div>
  );
}
