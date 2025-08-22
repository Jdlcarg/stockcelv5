import { useState, useMemo, useCallback } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Skeleton } from "@/components/ui/skeleton";
import { Product } from "@shared/schema";
import { Edit, Trash2, ArrowUpDown, ChevronUp, ChevronDown, History } from "lucide-react";
import EditProductModal from "./edit-product-modal";
import DeleteProductModal from "./delete-product-modal";
import { ProductHistoryModal } from "./product-history-modal";

export type SortField = 'imei' | 'model' | 'storage' | 'status' | 'costPrice' | 'battery' | 'provider' | 'entryDate';
export type SortDirection = 'asc' | 'desc';

interface ProductTableProps {
  products: Product[];
  isLoading?: boolean;
  sortField: SortField;
  sortDirection: SortDirection;
  onSort: (field: SortField) => void;
}

export default function ProductTable({ products, isLoading, sortField, sortDirection, onSort }: ProductTableProps) {
  const [editingProduct, setEditingProduct] = useState<Product | null>(null);
  const [deletingProduct, setDeletingProduct] = useState<Product | null>(null);
  const [historyProduct, setHistoryProduct] = useState<Product | null>(null);

  const getSortIcon = (field: SortField) => {
    if (sortField !== field) {
      return <ArrowUpDown className="h-4 w-4" />;
    }
    return sortDirection === 'asc' ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />;
  };

  const SortableHeader = ({ field, children }: { field: SortField; children: React.ReactNode }) => (
    <TableHead>
      <Button
        variant="ghost"
        onClick={() => onSort(field)}
        className="h-auto p-0 font-semibold hover:bg-transparent text-gray-900 dark:text-white"
      >
        <div className="flex items-center gap-2">
          {children}
          {getSortIcon(field)}
        </div>
      </Button>
    </TableHead>
  );

  const getStatusBadge = (status: string) => {
    switch (status) {
      case "disponible":
        return <Badge className="bg-accent/10 text-accent"> Disponible</Badge>;
      case "reservado":
        return <Badge className="bg-warning/10 text-warning">Reservado</Badge>;
      case "vendido":
        return <Badge className="bg-gray-100 text-gray-800">Vendido</Badge>;
      case "tecnico_interno":
        return <Badge className="bg-orange-100 text-orange-800">Técnico Interno</Badge>;
      case "tecnico_externo":
        return <Badge className="bg-red-100 text-red-800">Técnico Externo</Badge>;
      default:
        return <Badge variant="secondary">{status}</Badge>;
    }
  };

  if (isLoading) {
    return (
      <div className="space-y-4">
        {[...Array(5)].map((_, i) => (
          <Skeleton key={i} className="h-12 w-full" />
        ))}
      </div>
    );
  }

  if (products.length === 0) {
    return (
      <div className="text-center py-8">
        <p className="text-gray-500 dark:text-gray-400">No hay productos disponibles</p>
      </div>
    );
  }

  return (
    <div className="overflow-x-auto">
      <Table>
        <TableHeader>
          <TableRow>
            <SortableHeader field="imei">IMEI</SortableHeader>
            <SortableHeader field="model">Modelo</SortableHeader>
            <SortableHeader field="storage">Almacenamiento</SortableHeader>
            <SortableHeader field="status">Estado</SortableHeader>
            <SortableHeader field="costPrice">Precio Costo</SortableHeader>
            <SortableHeader field="battery">Batería</SortableHeader>
            <SortableHeader field="provider">Proveedor</SortableHeader>
            <SortableHeader field="entryDate">Fecha Ingreso</SortableHeader>
            <TableHead className="text-right text-gray-900 dark:text-white">Acciones</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {products.map((product) => (
            <TableRow key={product.id}>
              <TableCell className="font-medium text-gray-900 dark:text-white">{product.imei}</TableCell>
              <TableCell className="text-gray-900 dark:text-white">{product.model}</TableCell>
              <TableCell className="text-gray-900 dark:text-white">{product.storage}</TableCell>
              <TableCell>{getStatusBadge(product.status)}</TableCell>
              <TableCell className="text-gray-900 dark:text-white">${parseFloat(product.costPrice).toFixed(2)}</TableCell>
              <TableCell className="text-gray-900 dark:text-white">{product.battery || "N/A"}</TableCell>
              <TableCell className="text-gray-900 dark:text-white">{product.provider || "N/A"}</TableCell>
              <TableCell className="text-gray-900 dark:text-white">{product.entryDate ? new Date(product.entryDate).toLocaleDateString() : "N/A"}</TableCell>
              <TableCell className="text-right">
                <div className="flex items-center justify-end space-x-2">
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => setHistoryProduct(product)}
                    title="Ver historial"
                    className="text-green-600 hover:text-green-700"
                  >
                    <History className="h-4 w-4" />
                  </Button>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => setEditingProduct(product)}
                    title="Editar producto"
                  >
                    <Edit className="h-4 w-4" />
                  </Button>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => setDeletingProduct(product)}
                    title="Eliminar producto"
                    className="text-red-600 hover:text-red-700"
                  >
                    <Trash2 className="h-4 w-4" />
                  </Button>
                </div>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>

      {/* Modales */}
      {editingProduct && (
        <EditProductModal
          product={editingProduct}
          open={!!editingProduct}
          onOpenChange={(open) => !open && setEditingProduct(null)}
        />
      )}

      {deletingProduct && (
        <DeleteProductModal
          product={deletingProduct}
          open={!!deletingProduct}
          onOpenChange={(open) => !open && setDeletingProduct(null)}
        />
      )}

      {historyProduct && (
        <ProductHistoryModal
          isOpen={!!historyProduct}
          onClose={() => setHistoryProduct(null)}
          productId={historyProduct.id}
          productModel={historyProduct.model}
        />
      )}
    </div>
  );
}